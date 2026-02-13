package com.socrata.querycoordinator

import scala.concurrent.duration._
import java.io.InputStream
import java.util.concurrent.Executors
import com.socrata.querycoordinator.caching.cache.CacheCleanerThread
import com.socrata.querycoordinator.caching.cache.config.CacheSessionProviderFromConfig
import com.rojoma.json.v3.ast.JString
import com.rojoma.simplearm.v2._
import com.socrata.http.client.{HttpClient, HttpClientHttpClient, RequestBuilder}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.livenesscheck.LivenessCheckInfo
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.livenesscheck.LivenessCheckResponder
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server.util.handlers.{ErrorCatcher, LoggingOptions, NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.querycoordinator.caching.Windower
import com.socrata.querycoordinator.resources.{QueryResource, NewQueryResource, CacheStateResource, VersionResource}
import com.socrata.querycoordinator.util.TeeToTempInputStream
import com.socrata.querycoordinator.secondary_finder.{CachedSecondaryInstanceFinder, PrimingData}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{AnalysisSerializer, SoQLAnalyzer}
import com.socrata.curator.{ConfigWatch, CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.querycoordinator.rollups.{CompoundQueryRewriter, RollupInfoFetcher}
import com.socrata.thirdparty.metrics.{MetricsReporter, SocrataHttpSupport}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.curator.x.discovery.{ServiceProvider, ServiceInstanceBuilder, strategies}
import org.apache.log4j.PropertyConfigurator
import com.socrata.util.io.StreamWrapper
import org.slf4j.LoggerFactory

final abstract class Main

object Main extends App with DynamicPortMap {
  def withDefaultAddress(config: Config): Config = {
    val ifaces = ServiceInstanceBuilder.getAllLocalIPs
    if (ifaces.isEmpty) {
      config
    } else {
      val first = JString(ifaces.iterator.next().getHostAddress)
      val addressConfig =
        ConfigFactory.parseString("com.socrata.query-coordinator.service-advertisement.address=" + first)
      config.withFallback(addressConfig)
    }
  }

  val config = try {
    new QueryCoordinatorConfig(withDefaultAddress(ConfigFactory.load()), "com.socrata.query-coordinator")
  } catch {
    case e: Exception =>
      print(e.toString)
      sys.exit(1)
  }

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Main])

  ResourceScope.onLeak { rs =>
    try {
      log.warn("Resource scope {} leaked!", rs.name)
    } finally {
      rs.close()
    }
  }

  val analyzer = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)
  def typeSerializer(typ: SoQLType): String = typ.name.name
  val analysisSerializer = new AnalysisSerializer[String, SoQLType](identity, typeSerializer)

  implicit val execResource = Resource.executorShutdownNoTimeout

  def prime(
    http: HttpClient,
    discovery: ServiceProvider[AuxiliaryData],
    secondaryFinder: CachedSecondaryInstanceFinder[(NewQueryResource.DatasetInternalName, NewQueryResource.Stage), String]
  ): Unit = {
    try {
      for(instance <- Option(discovery.getInstance())) {
        val req =
          instance.toRequestBuilder
            .p("cache-state", "prime")
            .timeoutMS(Some(5000))
            .get
        for(resp <- http.execute(req)) {
          resp.resultCode match {
            case 200 =>
              resp.value[PrimingData[(NewQueryResource.DatasetInternalName, NewQueryResource.Stage), String]]() match {
                case Right(body) => secondaryFinder.prime(body)
                case Left(err) => log.warn("Undecodable response while priming cache: {}", err)
              }
            case non200 =>
              log.warn("Invalid response code while priming cache: {}", non200)
          }
        }
      }
    } catch {
      case e: Exception =>
        log.warn("Exception while priming cache from another instance", e)
    }
  }

  val threadPoolSize = 5
  for {
    executor <- managed(Executors.newFixedThreadPool(threadPoolSize))
    httpClientConfig <- unmanaged(HttpClientHttpClient.
      defaultOptions.
      withContentCompression(true).
      withUserAgent("Query Coordinator"))
    httpClient <- managed(new HttpClientHttpClient(executor, httpClientConfig))
    curator <- CuratorFromConfig(config.curator)
    discovery <- DiscoveryFromConfig(classOf[AuxiliaryData], curator, config.discovery)
    serviceProviderProvider <- managed(new ServiceProviderProvider(
      discovery,
      new strategies.RoundRobinStrategy))
    pongProvider <- managed(new LivenessCheckResponder(config.livenessCheck)).and(_.start())
    reporter <- MetricsReporter.managed(config.metrics)
    useBatchDelete <- managed(new ConfigWatch[Boolean](curator, "query-coordinator/use-batch-delete", true)).and(_.start())
    cacheSessionProvider <- CacheSessionProviderFromConfig(config.cache, useBatchDelete, StreamWrapper.gzip).and(_.init())
    cacheCleaner <- managed(new CacheCleanerThread(cacheSessionProvider, config.cache.cleanInterval, config.discovery.address)).and(_.start())
    scope <- managed(new ResourceScope)
  } {
    def teeStream(in: InputStream): TeeToTempInputStream = new TeeToTempInputStream(in)

    val secondaryInstanceSelector = new SecondaryInstanceSelector(config)

    val existenceChecker = ExistenceChecker(httpClient)

    val secondary = Secondary(
      secondaryProvider = serviceProviderProvider,
      existenceChecker = existenceChecker,
      secondaryInstance = secondaryInstanceSelector,
      connectTimeout = config.connectTimeout,
      schemaTimeout = config.schemaTimeout
    )

    val windower = new Windower(config.cache.rowsPerWindow)
    val maxWindowCount = config.cache.maxWindows
    val schemaFetcher = SchemaFetcher(httpClient)

    val secondaryFinder = scope.open(
      new CachedSecondaryInstanceFinder[(NewQueryResource.DatasetInternalName, NewQueryResource.Stage), String](
        config.allSecondaryInstanceNames.toSet,
        { case (secondaryName, (datasetName, stage)) =>
          Option(serviceProviderProvider.provider(secondaryName).getInstance()) match {
            case None =>
              CachedSecondaryInstanceFinder.CheckResult.Unknown
            case Some(instance) =>
              existenceChecker(secondary.reqBuilder(instance), datasetName.underlying, Some(stage.underlying)) match {
                case ExistenceChecker.Yes => CachedSecondaryInstanceFinder.CheckResult.Present
                case ExistenceChecker.No => CachedSecondaryInstanceFinder.CheckResult.Absent
                case e: ExistenceChecker.Error=>
                  log.warn("error checking for {}/{}'s existence: {}", secondaryName, datasetName, e)
                  CachedSecondaryInstanceFinder.CheckResult.Unknown
              }
          }
        },
        absentInterval = config.absentInterval,
        absentBound = config.absentBound,
        unknownInterval = config.unknownInterval,
        unknownBound = config.unknownBound
      )
    )
    secondaryFinder.start()
    prime(httpClient, serviceProviderProvider.provider(config.discovery.name), secondaryFinder)

    val queryResource = QueryResource(
      secondary = secondary,
      schemaFetcher = schemaFetcher,
      queryParser = new QueryParser(analyzer, schemaFetcher, config.maxRows, config.defaultRowsLimit),
      queryExecutor = new QueryExecutor(httpClient, analysisSerializer, teeStream, cacheSessionProvider, windower, maxWindowCount, config.maxDbQueryTimeout.toSeconds),
      connectTimeout = config.connectTimeout,
      schemaTimeout = config.schemaTimeout,
      receiveTimeout = config.receiveTimeout,
      schemaCache = (_, _, _) => (),
      schemaDecache = (_, _) => None,
      secondaryInstance = secondaryInstanceSelector,
      queryRewriter = new CompoundQueryRewriter(analyzer),
      rollupInfoFetcher = new RollupInfoFetcher(httpClient),
      secondaryInstanceFinder = secondaryFinder
    )

    val newQueryResource = new NewQueryResource(
      httpClient = httpClient,
      secondary = secondary,
      secondaryFinder = secondaryFinder,
      secondaryInstanceBroker = { secondary => Option(serviceProviderProvider.provider(secondary).getInstance()) }
    )

    val cacheStateResource = new CacheStateResource(secondaryFinder)

    val handler = Service(queryResource, newQueryResource, cacheStateResource, VersionResource())

    def remapLivenessCheckInfo(lci: LivenessCheckInfo): LivenessCheckInfo =
      new LivenessCheckInfo(hostPort(lci.port), lci.response)

    val auxData = new AuxiliaryData(Some(remapLivenessCheckInfo(pongProvider.livenessCheckInfo)))

    val logOptions = LoggingOptions(LoggerFactory.getLogger(""),
                                    logRequestHeaders = Set(ReqIdHeader, "X-Socrata-Resource"))

    val broker = new CuratorBroker(discovery, config.discovery.address, config.discovery.name,
                                   Some(auxData)) {
      override def register(port: Int): Cookie = {
        super.register(hostPort(port))
      }
    }

    val serv = new SocrataServerJetty(
      ThreadRenamingHandler(NewLoggingHandler(logOptions)(ErrorCatcher(handler))),
      SocrataServerJetty.defaultOptions.
                         withGzipOptions(
                           Some(
                             SocrataServerJetty.Gzip.defaultOptions.
                               withExcludedMimeTypes(
                                 Set("application/x-socrata-gzipped-cjson")
                               )
                           )
                         ).
                         withPort(config.network.port).
                         withExtraHandlers(List(SocrataHttpSupport.getHandler(config.metrics))).
                         withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
                         withBroker(broker)
    )
    log.info("Ready to go!  kicking off the server - downstream query servers {}...",
             config.allSecondaryInstanceNames.mkString("[", ", ", "]"))
    serv.run()
  }

  log.info("Terminated normally")
}

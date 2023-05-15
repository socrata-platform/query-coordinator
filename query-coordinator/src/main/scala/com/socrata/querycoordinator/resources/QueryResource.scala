package com.socrata.querycoordinator.resources

import java.io.{InputStream, OutputStream}
import javax.servlet.http.HttpServletResponse
import com.rojoma.simplearm.v2.ResourceScope
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.http.common.util.HttpUtils
import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId
import com.socrata.querycoordinator.rollups.QueryRewriter.{Analysis, AnalysisTree, ColumnId, RollupName}
import com.socrata.querycoordinator.SchemaFetcher.{NoSuchDatasetInSecondary, Successful, TimeoutFromSecondary}
import com.socrata.querycoordinator._
import com.socrata.querycoordinator.caching.SharedHandle
import com.socrata.querycoordinator.datetime.NowAnalyzer
import com.socrata.querycoordinator.exceptions.JoinedDatasetNotColocatedException
import com.socrata.querycoordinator.rollups.{QueryRewriter, RollupInfoFetcher, RollupInfo}
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.exceptions.{DuplicateAlias, NoSuchColumn, TypecheckException}
import com.socrata.soql.{BinaryTree, SoQLAnalysis}
import com.socrata.soql.types.{SoQLID, SoQLNumber, SoQLType}
import com.socrata.soql.stdlib.Context
import org.apache.http.HttpStatus
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration


case class QueryResource(secondary: Secondary,
                    mirror: Mirror,
                    schemaFetcher: SchemaFetcher,
                    queryParser: QueryParser,
                    queryExecutor: QueryExecutor,
                    connectTimeout: FiniteDuration,
                    schemaTimeout: FiniteDuration,
                    receiveTimeout: FiniteDuration,
                    schemaCache: (String, Option[String], Schema) => Unit,
                    schemaDecache: (String, Option[String]) => Option[Schema],
                    secondaryInstance: SecondaryInstanceSelector,
                    queryRewriter: QueryRewriter,
                    rollupInfoFetcher: RollupInfoFetcher) extends QCResource with QueryService { // scalastyle:ignore

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryResource])


  override def get: HttpRequest => HttpServletResponse => Unit = {
    post
  }

  override def put: HttpRequest => HttpServletResponse => Unit = {
    post
  }

  override def post: HttpRequest => HttpServletResponse => Unit = {
    req: HttpRequest => resp: HttpServletResponse =>
      process(req)(resp)
  }


  private def process(req: HttpRequest): HttpResponse = { // scalastyle:ignore cyclomatic.complexity method.length
    val originalThreadName = Thread.currentThread.getName
    val servReq = req.servletRequest
    try {
      Thread.currentThread.setName(Thread.currentThread.getId + " / " + req.method + " " +
        req.servletRequest.getRequestURI)

      val requestId = RequestId.getFromRequest(servReq)
      val dataset = Option(servReq.getParameter("ds")).getOrElse {
        finishRequest(noDatasetResponse)
      }
      val sfDataVersion = req.header("X-SODA2-DataVersion").map(_.toLong).get
      val sfLastModified = req.dateTimeHeader("X-SODA2-LastModified").get

      val forcedSecondaryName = Option(servReq.getParameter("store"))
      val noRollup = Option(servReq.getParameter("no_rollup")).isDefined
      val obfuscateId = !Option(servReq.getParameter(qpObfuscateId)).exists(_ == "false")

      forcedSecondaryName.foreach(ds => log.info("Forcing use of the secondary store instance: " + ds))

      val fuseMap: Map[String, String] = req.header("X-Socrata-Fuse-Columns").filter(_ != "")
                                            .map(parseFuseColumnMap(_))
                                            .getOrElse(Map.empty)

      val query = servReq.getParameter("q")
      val context = Option(servReq.getParameter("c")).fold(Context.empty) { cStr =>
        JsonUtil.parseJson[Context](cStr).right.get
      }

      val lensUid = Option(servReq.getParameter("lensUid"))
      val rowCount = Option(servReq.getParameter("rowCount"))
      val copy = Option(servReq.getParameter("copy"))
      val queryTimeoutSeconds = Option(servReq.getParameter(qpQueryTimeoutSeconds))
      val debug = req.header("X-Socrata-Debug").isDefined

      val explain = req.header("X-Socrata-Explain").isDefined
      val analyze = req.header("X-Socrata-Analyze").isDefined

      val precondition = req.precondition
      val ifModifiedSince = req.dateTimeHeader("If-Modified-Since")

      // A little spaghetti never hurt anybody!
      // Ok, so the general flow of this code is:
      //   1. Look up the dataset's schema (in cache or, if
      //     necessary, from the secondary)
      //   2. Analyze the query -- if this fails, and the failure
      //     was a type- or name-related error, and the schema came
      //     from the cache, refresh the schema and try again.
      //   3. Make the actual "give me data" request.
      // That last step is the most complex, because it is a
      // potentially long-running thing, and can cause a retry
      // if the upstream says "the schema just changed".

      final class QueryRetryState(retriesSoFar: Int, excludedSecondaryNames: Set[String]) {
        val chosenSecondaryName = secondary.chosenSecondaryName(forcedSecondaryName, dataset, copy, excludedSecondaryNames)

        val secondaryMirrorNames = chosenSecondaryName.map(mirror.secondaryMirrors).getOrElse(Nil)
        log.debug(s"Selected Mirrors for ${chosenSecondaryName}: $secondaryMirrorNames")

        val second = secondary.serviceInstance(dataset, chosenSecondaryName) match {
          case Some(x) => x
          case None =>
            finishRequest(noSecondaryAvailable(dataset))
        }

        val mirrorInstances = secondaryMirrorNames.flatMap(name => secondary.serviceInstance(dataset, Some(name), markBrokenOnUnknown = false))

        val base = secondary.reqBuilder(second)
        log.debug("Base URI: " + base.url)

        val mirrorBases = mirrorInstances.map(secondary.reqBuilder).toSet
        log.debug(s"Mirror URIs: ${mirrorBases.map(_.url)}")

        def checkTooManyRetries(): Unit = {
          if(isTooManyRetries) {
            log.error("Too many retries")
            finishRequest(ranOutOfRetriesResponse)
          }
        }

        def isTooManyRetries: Boolean = {
          retriesSoFar >= 3
        }

        def analyzeRequest(schemaWithFieldNames: Versioned[SchemaWithFieldName], isFresh: Boolean): Either[QueryRetryState, Versioned[(Schema, BinaryTree[SoQLAnalysis[String, SoQLType]])]] = {

          val schema0 = stripFieldNamesFromSchema(schemaWithFieldNames.payload)
          val schema =
            if (obfuscateId) schema0
            else schema0.copy(schema = schema0.schema.mapValues {
              case t: SoQLID.type => SoQLNumber
              case t => t
            })

          val columnIdMap = extractColumnIdMap(schemaWithFieldNames.payload)
          val parsedQuery = queryParser(query, columnIdMap, schema.schema, base, context, lensUid, fuseMap)

          parsedQuery match {
            case QueryParser.SuccessfulParse(analysis, largestLastModifiedOfJoins) =>
              val lastModified =
                if (schemaWithFieldNames.lastModified.isAfter(largestLastModifiedOfJoins)) {
                  schemaWithFieldNames.lastModified
                } else {
                  largestLastModifiedOfJoins
                }
              Right(schemaWithFieldNames.copy(payload = (schema, analysis), lastModified = lastModified))
            case QueryParser.AnalysisError(_: DuplicateAlias | _: NoSuchColumn | _: TypecheckException) if !isFresh =>
              getSchema(dataset, copy).right.flatMap(analyzeRequest(_, true))
            case QueryParser.AnalysisError(e) =>
              finishRequest(soqlErrorResponse(dataset, e))
            case QueryParser.UnknownColumnIds(cids) =>
              finishRequest(unknownColumnIds(cids))
            case QueryParser.RowLimitExceeded(max) =>
              finishRequest(rowLimitExceeded(max))
            case QueryParser.JoinedTableNotFound(j, s) =>
              val excludedSecondaries = excludedSecondaryNames ++ chosenSecondaryName.toSet
              val sec = secondary.chosenSecondaryName(forcedSecondaryName, dataset, copy, excludedSecondaries)
              if (secondary.serviceInstance(dataset, sec).isEmpty) {
                finishRequest(joinedTableNotFound(dataset, j, excludedSecondaries))
              } else {
                Left(nextRetry)
              }
            case QueryParser.ParameterSpecError(message) =>
              finishRequest(missingLensUid(message))
          }
        }

        def stripFieldNamesFromSchema(s: SchemaWithFieldName): Schema = {
          Schema(s.hash, s.schema.map { case (k, v) => (k -> v._1)}, s.pk)
        }

        def extractColumnIdMap(s: SchemaWithFieldName): Map[ColumnName, String] = {
          s.schema.map { case (k, v) => (new ColumnName(v._2) -> k)}
        }

        /**
         * @param analyzedQuery analysis which may have a rollup applied
         * @param analyzedQueryNoRollup original analysis without rollup
         */
        def executeQuery(schema: Versioned[Schema],
                         analyzedQuery: BinaryTree[SoQLAnalysis[String, SoQLType]],
                         analyzedQueryNoRollup: BinaryTree[SoQLAnalysis[String, SoQLType]],
                         context: Context,
                         rollupNames: Seq[String],
                         requestId: String,
                         resourceName: Option[String],
                         resourceScope: ResourceScope,
                         explain: Boolean,
                         analyze: Boolean): Either[QueryRetryState, HttpResponse] = {
          // Only pass in the primary rollup (associated with the primary table) to query server which does not know how to handle other rollups
          // not associated with the primary table when it is passed in the request header.  Other rollups are only embedded inside analyses
          // But when a query with any rollup fails, always retry without rollup.
          val primaryRollupName = QueryRewriter.primaryRollup(rollupNames)
          val extendedScope = resourceScope.open(SharedHandle(new ResourceScope))
          val extraHeaders = Map(RequestId.ReqIdHeader -> requestId,
                                 headerSocrataLastModified -> schema.lastModified.toHttpDate) ++
            resourceName.map(fbf => Map(headerSocrataResource -> fbf)).getOrElse(Nil) ++
            (if (analyze) List("X-Socrata-Analyze" -> "true" ) else Nil)
          val recvTimeout = queryTimeoutSeconds.map(x => Integer.parseInt(x) * 1000).getOrElse(receiveTimeout.toMillis.toInt)
          queryExecutor(
            base = base.receiveTimeoutMS(recvTimeout).connectTimeoutMS(connectTimeout.toMillis.toInt),
            dataset = dataset,
            mirrors = mirrorBases,
            analyses = analyzedQuery,
            schema = schema.payload,
            precondition = precondition,
            ifModifiedSince = ifModifiedSince,
            rowCount = rowCount,
            copy = copy,
            rollupName = primaryRollupName,
            context = context,
            obfuscateId = obfuscateId,
            extraHeaders = extraHeaders,
            currentCopyNumber = schema.copyNumber,
            currentDataVersion = schema.dataVersion,
            currentLastModified = schema.lastModified,
            resourceScopeHandle = extendedScope,
            queryTimeoutSeconds = queryTimeoutSeconds,
            debug = debug,
            explain = explain
          ) match {
            case QueryExecutor.Retry =>
              Left(nextRetry)
            case QueryExecutor.NotFound =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              Left(nextRetry)
            case QueryExecutor.Timeout =>
              // don't flag an error in this case because the timeout may be based on the particular query.
              finishRequest(upstreamTimeoutResponse)
            case QueryExecutor.SchemaHashMismatch(newSchema) =>
              storeInCache(Some(newSchema), dataset, copy)
              getSchema(dataset, copy).right.flatMap { schema =>
                analyzeRequest(schema, true).right.flatMap { versionedInfo =>
                  val (finalSchema, analyses) = versionedInfo.payload
                  val (rewrittenAnalyses, rollupName) = possiblyRewriteOneAnalysisInQuery(finalSchema, analyses)
                  executeQuery(versionedInfo.copy(payload = finalSchema),
                    rewrittenAnalyses, analyses, context, rollupName, requestId, resourceName, resourceScope, explain, analyze)
                }
              }
            case QueryExecutor.ToForward(responseCode, headers, body) =>
              // Log data version difference if response is OK.  Ignore not modified response and others.
              responseCode match {
                case HttpStatus.SC_OK =>
                  val lastModifiedOption: Option[DateTime] =
                  (headers("x-soda2-dataversion").headOption, headers("x-soda2-secondary-last-modified").headOption) match {
                    case (Some(qsDataVersion), Some(qsLastModified)) =>
                      val qsdv = qsDataVersion.toLong
                      val qslm = HttpUtils.parseHttpDate(qsLastModified)
                      val qslmOrlastModifiedJoin = if (qslm.isAfter(schema.lastModified)) qslm else schema.lastModified
                      logSchemaFreshness(second.getAddress, sfDataVersion, sfLastModified, qsdv, qslmOrlastModifiedJoin)
                      Some(qslmOrlastModifiedJoin)
                    case _ =>
                      log.warn("version related data not available from secondary")
                      None
                  }
                  val headersWithUpdatedLastModified = lastModifiedOption.map { x =>
                    headers + ("last-modified" -> Seq(ISODateTimeFormat.dateTime.print(x.getMillis)))
                  }.getOrElse(headers)
                  Right(transferHeaders(Status(responseCode), headersWithUpdatedLastModified) ~> Stream(out => transferResponse(out, body)))
                case HttpStatus.SC_INTERNAL_SERVER_ERROR if
                  (analyzedQuery.ne(analyzedQueryNoRollup) && rollupNames.nonEmpty) =>
                  // Rollup soql analysis passed but the sql asset behind in the secondary
                  // to support the rollup may be bad like missing rollup table.
                  // Retry w/o rollup to make queries more resilient.
                  log.warn(s"error in query with rollup ${rollupNames}.  retry w/o rollup - $body")
                  executeQuery(schema, analyzedQueryNoRollup, analyzedQueryNoRollup, context,
                               Nil, requestId, resourceName, resourceScope, explain, analyze)
                case _ =>
                  Right(transferHeaders(Status(responseCode), headers) ~> Stream(out => transferResponse(out, body)))
              }
            case QueryExecutor.InvalidJoin =>
              finishRequest(invalidJoinResponse)
          }
        }

        def getSchemaByTableName(tableName: TableName): SchemaWithFieldName = {
          val TableName(name, _) = tableName
          val schemaResult = schemaFetcher(base, name, None, useResourceName = true)
          schemaResult match {
            case Successful(schema, _, _, _) =>
              schema
            case NoSuchDatasetInSecondary =>
              throw new JoinedDatasetNotColocatedException(name, base.host)
            case TimeoutFromSecondary =>
              finishRequest(upstreamTimeoutResponse)
            case other: SchemaFetcher.Result =>
              log.error(unexpectedError, s"${other} $name ${base.host}")
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(internalServerError)
          }
        }

        def fetchRollupInfo(): Seq[RollupInfo] = rollupInfoFetcher(base.receiveTimeoutMS(schemaTimeout.toMillis.toInt), Left(dataset), copy) match {
          case RollupInfoFetcher.Successful(rollups) =>
            rollups
          case RollupInfoFetcher.NoSuchDatasetInSecondary =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(notFoundResponse(dataset))
          case RollupInfoFetcher.TimeoutFromSecondary =>
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(upstreamTimeoutResponse)
          case other: RollupInfoFetcher.Result =>
            log.error(unexpectedError, other)
            chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
            finishRequest(internalServerError)
          case unsuccessful =>
            log.warn(s"No rollups ${dataset} ${unsuccessful.toString}")
            Seq.empty
        }

        /**
         * Scan from left to right (inner to outer), rewrite the first possible one.
         * TODO: Find a better way to apply rollup?
         */
        def possiblyRewriteOneAnalysisInQuery(schema: Schema, analyzedQuery: BinaryTree[SoQLAnalysis[String, SoQLType]],
                                              ruMapOpt: Option[Map[RollupName, AnalysisTree]] = None):
        (BinaryTree[SoQLAnalysis[String, SoQLType]], Seq[String]) = {
          if (noRollup) {
            (analyzedQuery, Seq.empty)
          } else {
            queryRewriter.possiblyRewriteOneAnalysisInQuery(
              dataset, schema, analyzedQuery, ruMapOpt, fetchRollupInfo, getSchemaByTableName, debug
            )
          }
        }

        case class Versioned[T](payload: T, copyNumber: Long, dataVersion: Long, lastModified: DateTime)

        def nextRetry: QueryRetryState = {
          checkTooManyRetries()
          new QueryRetryState(retriesSoFar + 1, chosenSecondaryName.map(x => excludedSecondaryNames + x).getOrElse(excludedSecondaryNames))
        }

        def getSchema(dataset: String, copy: Option[String]): Either[QueryRetryState, Versioned[SchemaWithFieldName]] = {
          schemaFetcher(
            base.receiveTimeoutMS(schemaTimeout.toMillis.toInt).connectTimeoutMS(connectTimeout.toMillis.toInt),
            dataset,
            copy) match {
            case SchemaFetcher.SecondaryConnectFailed =>
              Left(nextRetry)
            case SchemaFetcher.Successful(s, c, d, l) =>
              Right(Versioned(s, c, d, l))
            case SchemaFetcher.NoSuchDatasetInSecondary =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              Left(nextRetry)
            case SchemaFetcher.TimeoutFromSecondary =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(upstreamTimeoutResponse)
            case other: SchemaFetcher.Result =>
              log.error(unexpectedError, other)
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(internalServerError)
          }
        }

        def go(): Either[QueryRetryState, HttpResponse] = {
          getSchema(dataset, copy).right.flatMap(analyzeRequest(_, true)).right.flatMap { versionInfo =>
            val (finalSchema, analyses) = versionInfo.payload
            val (rewrittenAnalyses, rollupName) = possiblyRewriteOneAnalysisInQuery(finalSchema, analyses)
            val now = (new NowAnalyzer(rewrittenAnalyses)).getNow()
            val (largestLastModified, contextWithNow) = now match {
              case Some(x) if x.isAfter(versionInfo.lastModified) =>
                (x.toDateTime(DateTimeZone.UTC), context.augmentSystemContext(contextVarNow, x.getMillis.toString))
              case _ =>
                (versionInfo.lastModified, context)
            }
            executeQuery(versionInfo.copy(payload = finalSchema, lastModified = largestLastModified),
              rewrittenAnalyses, analyses, contextWithNow, rollupName,
              requestId, req.header(headerSocrataResource), req.resourceScope, explain, analyze)
          }
        }
      }

      @tailrec
      def loop(qs: QueryRetryState): HttpResponse = {
        qs.go() match {
          case Left(qs2) => loop(qs2)
          case Right(r) => r
        }
      }
      loop(new QueryRetryState(0, Set.empty))
    } catch {
      case FinishRequest(response) =>
        response
    } finally {
      Thread.currentThread.setName(originalThreadName)
    }
  }

  private def transferResponse(out: OutputStream, in: InputStream): Unit = {
    val buf = new Array[Byte](responseBuffer)
    @annotation.tailrec
    def loop(): Unit = {
      in.read(buf) match {
        case -1 => // done
        case n: Int =>
          out.write(buf, 0, n)
          loop()
      }
    }
    loop()
  }


  private def storeInCache(schema: Option[Schema], dataset: String, copy: Option[String]): Option[Schema] = {
    schema match {
      case s@Some(trueSchema) =>
        schemaCache(dataset, copy, trueSchema)
        s
      case None =>
        None
    }
  }

  private def logSchemaFreshness(secondaryAddress: String,
                                 sfDataVersion: Long,
                                 sfLastModified: DateTime,
                                 qsDataVersion: Long,
                                 qsLastModified: DateTime): Unit = {
    val DataVersionDiff = sfDataVersion - qsDataVersion
    val wrongOrder = qsLastModified.isAfter(sfLastModified)
    val timeDiff = (if (wrongOrder) {
      new Interval(sfLastModified, qsLastModified)
    } else {
      new Interval(qsLastModified, sfLastModified)
    }).toDuration
    if (DataVersionDiff == 0 && timeDiff.getMillis == 0) {
      log.debug("schema from {} is identical", secondaryAddress)
    } else {
      log.debug("schema from {} differs {} {} {} {} {}v {}{}m",
        secondaryAddress,
        sfDataVersion.toString,
        sfLastModified.toString(ISODateTimeFormat.dateHourMinuteSecond),
        qsDataVersion.toString,
        qsLastModified.toString(ISODateTimeFormat.dateHourMinuteSecond),
        DataVersionDiff.toString,
        if (wrongOrder) qpHyphen else "",
        timeDiff.getStandardMinutes.toString)
    }
  }


  private def transferHeaders(resp: HttpResponse, headers: Map[String, Seq[String]]): HttpResponse = {
    headers.foldLeft(resp) { case (acc, (h: String, vs: Seq[String])) =>
      vs.foldLeft(acc) { (acc2, v: String) =>
        acc2 ~> Header(h, v)
      }
    }
  }

  /**
   * Parse "loc_column,location;phone_column,phone" into a map
   */
  private def parseFuseColumnMap(s: String): Map[String, String] = {
    s.split(';')
     .map { item => item.split(',') }
     .map { case Array(a, b) => (a, b)
            case _ =>
               throw new Exception(s"X-Socrata-Fuse-Columns parse error $s.")
     }
     .toMap
  }
}

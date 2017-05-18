package com.socrata.querycoordinator.resources

import java.io.{InputStream, OutputStream}
import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.io.JsonReaderException
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.common.util.HttpUtils
import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId
import com.socrata.querycoordinator._
import com.socrata.querycoordinator.caching.SharedHandle
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.exceptions.{DuplicateAlias, NoSuchColumn, TypecheckException}
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed.ColumnRef
import com.socrata.soql.types.SoQLType
import org.apache.http.HttpStatus
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, Interval}


import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration


class QueryResource(secondary: Secondary,
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

      val fuseMap: Map[String, String] = req.header("X-Socrata-Fuse-Columns")
                                            .map(parseFuseColumnMap(_))
                                            .getOrElse(Map.empty)

      val query = servReq.getParameter("q")

      val rowCount = Option(servReq.getParameter("rowCount"))
      val copy = Option(servReq.getParameter("copy"))
      val queryTimeoutSeconds = Option(servReq.getParameter(qpQueryTimeoutSeconds))

      val jsonizedColumnIdMap = Option(servReq.getParameter("idMap")).getOrElse {
        finishRequest(BadRequest ~> Json("no idMap provided"))
      }
      val columnIdMap: Map[ColumnName, String] = try {
        JsonUtil.parseJson[Map[String, String]](jsonizedColumnIdMap) match {
          case Right(rawMap) =>
            rawMap.map { case (col, typ) => ColumnName(col) -> typ }
          case Left(_) =>
            finishRequest(BadRequest ~> Json("idMap not an object whose values are strings"))
        }
      } catch {
        case e: JsonReaderException =>
          finishRequest(BadRequest ~> Json("idMap not parsable as JSON"))
      }
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

      final class QueryRetryState(retriesSoFar: Int) {
        val chosenSecondaryName = secondary.chosenSecondaryName(forcedSecondaryName, dataset, copy)
        val second = secondary.serviceInstance(dataset, chosenSecondaryName)
        val base = secondary.reqBuilder(second)
        log.debug("Base URI: " + base.url)

        def analyzeRequest(schema: Versioned[Schema], isFresh: Boolean): Either[QueryRetryState, Versioned[(Schema, Seq[SoQLAnalysis[String, SoQLType]])]] = {

          val parsedQuery = queryParser(query, columnIdMap, schema.payload.schema, base, fuseMap)

          parsedQuery match {
            case QueryParser.SuccessfulParse(analysis) =>
              Right(schema.copy(payload = (schema.payload, analysis)))
            case QueryParser.AnalysisError(_: DuplicateAlias | _: NoSuchColumn | _: TypecheckException) if !isFresh =>
              getSchema(dataset, copy).right.flatMap(analyzeRequest(_, true))
            case QueryParser.AnalysisError(e) =>
              finishRequest(soqlErrorResponse(dataset, e))
            case QueryParser.UnknownColumnIds(cids) =>
              finishRequest(unknownColumnIds(cids))
            case QueryParser.RowLimitExceeded(max) =>
              finishRequest(rowLimitExceeded(max))
          }
        }

        /**
         * @param analyzedQuery analysis which may have a rollup applied
         * @param analyzedQueryNoRollup original analysis without rollup
         */
        def executeQuery(schema: Versioned[Schema],
                         analyzedQuery: Seq[SoQLAnalysis[String, SoQLType]],
                         analyzedQueryNoRollup: Seq[SoQLAnalysis[String, SoQLType]],
                         rollupName: Option[String],
                         requestId: String,
                         resourceName: Option[String],
                         resourceScope: ResourceScope): Either[QueryRetryState, HttpResponse] = {
          val extendedScope = resourceScope.open(SharedHandle(new ResourceScope))
          val extraHeaders = Map(RequestId.ReqIdHeader -> requestId) ++
            resourceName.map(fbf => Map(headerSocrataResource -> fbf)).getOrElse(Nil)
          queryExecutor(
            base.receiveTimeoutMS(receiveTimeout.toMillis.toInt).connectTimeoutMS(connectTimeout.toMillis.toInt),
            dataset,
            analyzedQuery,
            schema.payload,
            precondition,
            ifModifiedSince,
            rowCount,
            copy,
            rollupName,
            obfuscateId,
            extraHeaders,
            schema.copyNumber,
            schema.dataVersion,
            schema.lastModified,
            extendedScope,
            queryTimeoutSeconds
          ) match {
            case QueryExecutor.Retry =>
              Left(nextRetry)
            case QueryExecutor.NotFound =>
              chosenSecondaryName.foreach { n => secondaryInstance.flagError(dataset, n) }
              finishRequest(notFoundResponse(dataset))
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
                    rewrittenAnalyses, analyses, rollupName, requestId, resourceName, resourceScope)
                }
              }
            case QueryExecutor.ToForward(responseCode, headers, body) =>
              // Log data version difference if response is OK.  Ignore not modified response and others.
              responseCode match {
                case HttpStatus.SC_OK =>
                  (headers("x-soda2-dataversion").headOption, headers("x-soda2-secondary-last-modified").headOption) match {
                    case (Some(qsDataVersion), Some(qsLastModified)) =>
                      val qsdv = qsDataVersion.toLong
                      val qslm = HttpUtils.parseHttpDate(qsLastModified)
                      logSchemaFreshness(second.getAddress, sfDataVersion, sfLastModified, qsdv, qslm)
                    case _ =>
                      log.warn("version related data not available from secondary")
                  }
                  Right(transferHeaders(Status(responseCode), headers) ~> Stream(out => transferResponse(out, body)))
                case HttpStatus.SC_INTERNAL_SERVER_ERROR if
                  (analyzedQuery.ne(analyzedQueryNoRollup) && rollupName.isDefined) =>
                  // Rollup soql analysis passed but the sql asset behind in the secondary
                  // to support the rollup may be bad like missing rollup table.
                  // Retry w/o rollup to make queries more resilient.
                  log.warn(s"error in query with rollup ${rollupName.get}.  retry w/o rollup - $body")
                  executeQuery(schema, analyzedQueryNoRollup, analyzedQueryNoRollup,
                               None, requestId, resourceName, resourceScope)
                case _ =>
                  Right(transferHeaders(Status(responseCode), headers) ~> Stream(out => transferResponse(out, body)))
              }
          }
        }

        /**
         * Scan from left to right (inner to outer), rewrite the first possible one.
         * TODO: Find a better way to apply rollup?
         */
        def possiblyRewriteOneAnalysisInQuery(schema: Schema, analyzedQuery: Seq[SoQLAnalysis[String, SoQLType]])
          : (Seq[SoQLAnalysis[String, SoQLType]], Option[String]) = {
          if (noRollup || analyzedQuery.exists(_.join.nonEmpty)) {
            (analyzedQuery, None)
          } else {
            rollupInfoFetcher(base.receiveTimeoutMS(schemaTimeout.toMillis.toInt), dataset, copy) match {
              case RollupInfoFetcher.Successful(rollups) =>
                // Only the leftmost soql in a chain can use rollups.
                possiblyRewriteQuery(schema, analyzedQuery.head, rollups, Map.empty[String, String]) match {
                  case (rewrittenAnal, ru@Some(_)) =>
                    (analyzedQuery.updated(0, rewrittenAnal), ru)
                  case (_, None) =>
                    (analyzedQuery, None)
                }
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
            }
          }
        }

        def possiblyRewriteQuery(schema: Schema, analyzedQuery: SoQLAnalysis[String, SoQLType], rollups: Seq[RollupInfo], project: Map[String, String])
          : (SoQLAnalysis[String, SoQLType], Option[String]) = {
          val rewritten = RollupScorer.bestRollup(
            queryRewriter.possibleRewrites(schema, analyzedQuery, rollups, project).toSeq)
          val (rollupName, analysis) = rewritten map { x => (Some(x._1), x._2) } getOrElse ((None, analyzedQuery))
          rollupName.foreach(ru => log.info(s"Rewrote query on dataset $dataset to rollup $ru")) // only log rollup name if it is defined.
          log.debug(s"Rewritten analysis: $analysis")
          (analysis, rollupName)
        }

        case class Versioned[T](payload: T, copyNumber: Long, dataVersion: Long, lastModified: DateTime)

        def nextRetry: QueryRetryState =
          if(retriesSoFar < 3) {
            new QueryRetryState(retriesSoFar + 1)
          } else {
            log.error("Too many retries")
            finishRequest(ranOutOfRetriesResponse)
          }

        def getSchema(dataset: String, copy: Option[String]): Either[QueryRetryState, Versioned[Schema]] = {
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
              finishRequest(notFoundResponse(dataset))
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
            executeQuery(versionInfo.copy(payload = finalSchema),
              rewrittenAnalyses, analyses, rollupName,
              requestId, req.header(headerSocrataResource), req.resourceScope)
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
      loop(new QueryRetryState(0))
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
      log.info("schema from {} is identical", secondaryAddress)
    } else {
      log.info("schema from {} differs {} {} {} {} {}v {}{}m",
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
     .map { case Array(a, b) => (a, b) }
     .toMap
  }
}

object QueryResource {
  def apply(secondary: Secondary, // scalastyle:ignore
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
            rollupInfoFetcher: RollupInfoFetcher): QueryResource = {
    new QueryResource(secondary,
      schemaFetcher,
      queryParser,
      queryExecutor,
      connectTimeout,
      schemaTimeout,
      receiveTimeout,
      schemaCache,
      schemaDecache,
      secondaryInstance,
      queryRewriter,
      rollupInfoFetcher
    )
  }
}

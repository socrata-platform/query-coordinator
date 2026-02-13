package com.socrata.querycoordinator.resources

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.util.Random

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.net.SocketException

import com.rojoma.json.v3.ast.{JBoolean, JString, JValue, JNull}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{WrapperJsonCodec, AutomaticJsonCodec, NullForNone, AllowMissing, SimpleHierarchyCodecBuilder, InternalTag, AutomaticJsonCodecBuilder, JsonUtil}
import com.rojoma.simplearm.v2.ResourceScope
import org.apache.commons.io.HexDump
import org.apache.curator.x.discovery.ServiceInstance
import org.slf4j.LoggerFactory
import org.joda.time.{DateTime, LocalDateTime}

import com.socrata.http.client.{HttpClient, Response}
import com.socrata.http.client.exceptions.{ConnectTimeout, ConnectFailed}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.ext.{ResourceExt, Json, HeaderMap, HeaderName, RequestId}
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.http.server.responses.{Stream => StreamBytes, Json => JsonResp, _}
import com.socrata.http.server.implicits._
import com.socrata.soql.analyzer2.{MetaTypes, FoundTables, SoQLAnalyzer, UserParameters, DatabaseTableName, DatabaseColumnName, SoQLAnalysis, CanonicalName, AggregateFunctionCall, FuncallPositionInfo, SoQLAnalyzerError, types}
import com.socrata.soql.analyzer2.rewrite.Pass
import com.socrata.soql.environment.{HoleName, ColumnName, Provenance}
import com.socrata.soql.serialize.{WriteBuffer, Writable}
import com.socrata.soql.types.{SoQLType, SoQLValue, SoQLFixedTimestamp, SoQLFloatingTimestamp}
import com.socrata.soql.functions.{MonomorphicFunction, SoQLTypeInfo, SoQLFunctionInfo, SoQLFunctions}
import com.socrata.soql.stdlib.analyzer2.{Context, PreserveSystemColumnsAggregateMerger}
import com.socrata.soql.sql.Debug
import com.socrata.soql.util.GenericSoQLError

import com.socrata.querycoordinator.Secondary
import com.socrata.querycoordinator.secondary_finder.{FoundSecondary, SecondaryInstanceFinder}
import com.socrata.querycoordinator.util.{Lazy, NewQueryError}

object NewQueryResource {
  val log = LoggerFactory.getLogger(classOf[NewQueryResource])

  case class DatasetInternalName(underlying: String)
  object DatasetInternalName {
    implicit val codec = WrapperJsonCodec[DatasetInternalName](apply(_), _.underlying)
    implicit object serialize extends Writable[DatasetInternalName] {
      def writeTo(buffer: WriteBuffer, s: DatasetInternalName) = buffer.write(s.underlying)
    }
  }

  case class Stage(underlying: String)
  object Stage {
    def apply(underlying: String) = new Stage(underlying.intern())

    implicit val codec = WrapperJsonCodec[Stage]({ s: String => Stage(s.toLowerCase) }, _.underlying)
    implicit object serialize extends Writable[Stage] {
      def writeTo(buffer: WriteBuffer, s: Stage) = buffer.write(s.underlying)
    }
  }

  case class ColumnId(underlying: String)
  object ColumnId {
    implicit val codec = WrapperJsonCodec[ColumnId](apply(_), _.underlying)
    implicit object serialize extends Writable[ColumnId] {
      def writeTo(buffer: WriteBuffer, cid: ColumnId) = buffer.write(cid.underlying)
    }
  }

  final abstract class MT extends MetaTypes {
    override type ResourceNameScope = Int
    override type ColumnType = SoQLType
    override type ColumnValue = SoQLValue
    override type DatabaseTableNameImpl = (DatasetInternalName, Stage)
    override type DatabaseColumnNameImpl = ColumnId
  }

  object MT {
    object ToProvenance extends types.ToProvenance[MT] {
      // This serialization must be decodable on the other side
      override def toProvenance(dtn: types.DatabaseTableName[MT]) =
        Provenance(JsonUtil.renderJson(dtn.name, pretty = false))
    }
  }

  val analyzer = new SoQLAnalyzer[MT](SoQLTypeInfo.soqlTypeInfo2, SoQLFunctionInfo, MT.ToProvenance)
  val systemColumnNonAggregatePreservingAnalyzer = analyzer.preserveSystemColumns((_, _) => None)
  val systemColumnPreservingAnalyzer = analyzer
    .preserveSystemColumns(PreserveSystemColumnsAggregateMerger())

  implicit val analyzerErrorCodecs =
    SoQLAnalyzerError.errorCodecs[MT#ResourceNameScope, SoQLAnalyzerError[MT#ResourceNameScope]]().
      build


  case class AuxTableData(
    locationSubcolumns: Map[types.DatabaseColumnName[MT], Seq[Option[types.DatabaseColumnName[MT]]]],
    sfResourceName: String,
    truthDataVersion: Long
  )
  object AuxTableData {
    implicit val serialize = new Writable[AuxTableData] {
      def writeTo(buffer: WriteBuffer, atd: AuxTableData): Unit = {
        buffer.write(0)
        buffer.write(atd.locationSubcolumns)
        buffer.write(atd.sfResourceName)
        buffer.write(atd.truthDataVersion)
      }
    }

    implicit val jCodec = new JsonEncode[AuxTableData] with JsonDecode[AuxTableData] {
      @AutomaticJsonCodec
      case class AuxTableDataRaw(
        locationSubcolumns: Seq[(types.DatabaseColumnName[MT], Seq[Either[JNull, types.DatabaseColumnName[MT]]])],
        sfResourceName: String,
        truthDataVersion: Long
      )

      def encode(v: AuxTableData) =
        JsonEncode.toJValue(
          AuxTableDataRaw(
            v.locationSubcolumns.mapValues(_.map(_.toRight(JNull))).toSeq,
            v.sfResourceName,
            v.truthDataVersion
          )
        )

      def decode(v: JValue) =
        JsonDecode.fromJValue[AuxTableDataRaw](v).map { v =>
          AuxTableData(
            v.locationSubcolumns.toMap.mapValues(_.map { case Right(cid) => Some(cid); case Left(JNull) => None }),
            v.sfResourceName,
            v.truthDataVersion
          )
        }
    }
  }

  private case class Request(
    analysis: SoQLAnalysis[MT],
    auxTableData: Map[types.DatabaseTableName[MT], AuxTableData],
    systemContext: Map[String, String],
    rewritePasses: Seq[Seq[Pass]],
    allowRollups: Boolean,
    debug: Option[Debug],
    queryTimeout: Option[FiniteDuration]
  )
  private object Request {
    implicit def serialize(implicit ev: Writable[SoQLAnalysis[MT]]) = new Writable[Request] {
      def writeTo(buffer: WriteBuffer, req: Request): Unit = {
        buffer.write(2)
        buffer.write(req.analysis)
        buffer.write(req.auxTableData)
        buffer.write(req.systemContext)
        buffer.write(req.rewritePasses)
        buffer.write(req.allowRollups)
        buffer.write(req.debug)
        buffer.write(req.queryTimeout.map(_.toMillis))
      }
    }
  }

  sealed abstract class PreserveSystemColumns
  object PreserveSystemColumns {
    case object Never extends PreserveSystemColumns
    case object NonAggregated extends PreserveSystemColumns
    case object Always extends PreserveSystemColumns

    implicit object jCodec extends JsonEncode[PreserveSystemColumns] with JsonDecode[PreserveSystemColumns] {
      def encode(psc: PreserveSystemColumns) =
        psc match {
          case Never => JBoolean(false)
          case NonAggregated => JString("non_aggregated")
          case Always => JBoolean(true)
        }
      def decode(x: JValue) =
        x match {
          case JBoolean(false) | JString("never") => Right(Never)
          case JBoolean(true) | JString("always") => Right(Always)
          case JString("non_aggregated") => Right(NonAggregated)
          case other@JString(_) => Left(DecodeError.InvalidValue(other))
          case other => Left(DecodeError.join(
                               List(
                                 DecodeError.InvalidType(expected = JString, got = other.jsonType),
                                 DecodeError.InvalidType(expected = JBoolean, got = other.jsonType)
                               )))
        }
    }
  }

  @AutomaticJsonCodec
  case class Body(
    foundTables: FoundTables[MT],
    // Release migration: accept a top-level locationSubcolumns if
    // it's provided; once this and SF are released, this field can go
    // away.
    @AllowMissing("Nil")
    locationSubcolumns: Seq[(types.DatabaseTableName[MT], Seq[(types.DatabaseColumnName[MT], Seq[Either[JNull, types.DatabaseColumnName[MT]]])])],
    @AllowMissing("Nil")
    auxTableData: Seq[(types.DatabaseTableName[MT], AuxTableData)],
    @AllowMissing("Context.empty")
    context: Context,
    @AllowMissing("Nil")
    rewritePasses: Seq[Seq[Pass]],
    @AllowMissing("true")
    allowRollups: Boolean,
    @AllowMissing("PreserveSystemColumns.Never")
    preserveSystemColumns: PreserveSystemColumns,
    debug: Option[Debug],
    queryTimeoutMS: Option[Long],
    store: Option[String]
  )

  private implicit lazy val errorCodecs =
    NewQueryError.errorCodecs[MT#ResourceNameScope, NewQueryError[MT#ResourceNameScope]]().build
}

class NewQueryResource(
  httpClient: HttpClient,
  // A thing that can say "this dataset is in these secondaries"
  secondaryFinder: SecondaryInstanceFinder[(NewQueryResource.DatasetInternalName, NewQueryResource.Stage), String],
  // A thing that can say "here is a registration record for this secondary"
  secondaryInstanceBroker: String => Option[ServiceInstance[AuxiliaryData]],
  // shared with the old codepath; we use it for building the base RequestBuilder
  secondary: Secondary
) extends QCResource with ResourceExt {
  import NewQueryResource._

  def doit(rs: ResourceScope, headers: HeaderMap, reqId: RequestId, json: Json[Body]): HttpResponse = {
    val Json(
      reqData@Body(
        foundTables,
        locationSubcolumnsRaw,
        auxTableDataRaw,
        context,
        rewritePasses,
        allowRollups,
        preserveSystemColumns,
        debug,
        queryTimeoutMS,
        store
      )
    ) = json

    log.debug("Received request {}", Lazy(JsonUtil.renderJson(reqData, pretty = true)))

    val auxTableData =
      if(auxTableDataRaw.nonEmpty) {
        auxTableDataRaw.toMap
      } else {
        locationSubcolumnsRaw.toMap.mapValues { locs =>
          AuxTableData(locs.toMap.mapValues(_.map { case Right(cid) => Some(cid); case Left(JNull) => None }), "", -1)
        }
      }

    val effectiveAnalyzer =
      preserveSystemColumns match {
        case PreserveSystemColumns.Always => systemColumnPreservingAnalyzer
        case PreserveSystemColumns.NonAggregated => systemColumnNonAggregatePreservingAnalyzer
        case PreserveSystemColumns.Never => analyzer
      }

    effectiveAnalyzer(foundTables, context.user.toUserParameters) match {
      case Right(analysis) =>
        val serialized = WriteBuffer.asBytes(Request(analysis, auxTableData, context.system, rewritePasses, allowRollups, debug, queryTimeoutMS.map(_.milliseconds)))

        log.debug("Serialized analysis as:\n{}", Lazy(locally {
          val baos = new ByteArrayOutputStream
          HexDump.dump(serialized, 0, baos, 0)
          new String(baos.toByteArray, StandardCharsets.ISO_8859_1)
        }))

        def bail(err: NewQueryError[MT#ResourceNameScope]) = BadRequest ~> JsonResp(err)

        val additionalHeaders = Seq("if-none-match", "if-match", "if-modified-since").flatMap { h =>
          headers.lookup(HeaderName(h)).map { v => h -> v.underlying }
        }

        // This is only entered if there is at least one chosen secondary
        @tailrec
        def issueQuery(retries: Int, remainingSecondaries: Queue[FoundSecondary[String]]): Either[HttpResponse, (FoundSecondary[String], Response)] = {
          remainingSecondaries.dequeueOption match {
            case None =>
              // All chosen secondaries failed to find an instance;
              // bail out with an error
              Left(bail(NewQueryError.SecondarySelectionError.NoSecondariesForAllDatasets()))
            case Some((chosenSecondary, remainingSecondaries)) =>
              secondaryInstanceBroker(chosenSecondary.secondary) match {
                case None =>
                  // there were no instances for the chosen secondary,
                  // don't count this as a "retry", but also don't
                  // re-enqueue the chosen secondary.
                  issueQuery(retries, remainingSecondaries)
                case Some(instance) =>
                  val req = secondary.reqBuilder(instance)
                    .p("new-query")
                    .addHeaders(additionalHeaders)
                    .addHeader(ReqIdHeader, reqId.toString)
                    .blob(new ByteArrayInputStream(serialized))

                  val respOrRetriableError = try {
                    Right((chosenSecondary, httpClient.execute(req, rs)))
                  } catch {
                    case e: ConnectTimeout => Left(e)
                    case e: ConnectFailed => Left(e)
                    case e: SocketException => Left(e)
                  }

                  respOrRetriableError match {
                    case Right(result) => Right(result)
                    case Left(_) if retries < 3 => issueQuery(retries + 1, remainingSecondaries.enqueue(chosenSecondary))
                    case Left(e) => throw e
                  }
              }
          }
        }

        def chooseSecondaries(): Either[HttpResponse, Seq[FoundSecondary[String]]] = {
          store match {
            case None =>
              val tables = analysis.statement.allTables.map(_.name)
              if(tables.isEmpty) {
                // no tables involved, so we don't care what secondary we use!
                Right(Random.shuffle(secondaryFinder.allSecondaries.toSeq))
              } else {
                var candidates = secondaryFinder.whereAre(tables)

                // This isn't great, but it also shouldn't be a thing
                // that happens much/at all.  If we didn't find any
                // candidates, then either
                //   * The user is querying datasets that do not live
                //     together, which shouldn't be a thing that can
                //     happen
                //   * Datasets have moved and our cache is out of
                //     date, which can happen _rarely_.
                // So this assumes the latter, and thus clears any
                // cached data for these tables and rescans.
                if(candidates.isEmpty) {
                  log.info("Secondary finder said there were no secondaries where {} were all located; invalidating and trying again", tables)
                  secondaryFinder.softInvalidate(tables)
                  candidates = secondaryFinder.whereAre(tables)
                  if(candidates.isEmpty) {
                    log.warn("Secondary finder still said there were no secondaries where {} were all located!", tables)
                    if(tables.size == 1) {
                      return Left(bail(NewQueryError.SecondarySelectionError.NoSecondariesForDataset(foundTables.tableMap.byDatabaseTableName(DatabaseTableName(tables.head)).head)))
                    } else {
                      return Left(bail(NewQueryError.SecondarySelectionError.NoSecondariesForAllDatasets()))
                    }
                  }
                }

                Right(Random.shuffle(candidates.toSeq))
              }
            case Some(store) =>
              log.info("Forcing secondary to {}", JString(store))
              Right(Seq(new FoundSecondary.Simple(store)))
          }
        }

        @tailrec
        def retryLoop(retries: Int): HttpResponse = {
          val chosenSecondaries: Seq[FoundSecondary[String]] =
            chooseSecondaries() match {
              case Right(cs) => cs
              case Left(response) => return response
            }

          issueQuery(0, Queue(chosenSecondaries : _*)) match {
            case Right((chosenSecondary, resp)) =>
              val base = Status(resp.resultCode) ~> Header("x-soda2-secondary", chosenSecondary.secondary)

              resp.resultCode match {
                case code@(200 | 304) =>
                  Seq("content-type", "etag", "last-modified", "x-soda2-data-out-of-date", "x-soda2-cached").foldLeft[HttpResponse](base) { (acc, hdrName) =>
                    resp.headers(hdrName).foldLeft(acc) { (acc, value) =>
                      acc ~> Header(hdrName, value)
                    }
                  } ~> StreamBytes(resp.inputStream())

                case 404 if resp.headers("X-Socrata-Missing-Dataset").nonEmpty =>
                  if(retries >= 3) {
                    throw new Exception("Repeatedly got told a secondary contained tables but when we issued the query it didn't?")
                  }
                  rs.close(resp)
                  for(table <- analysis.statement.allTables) {
                    chosenSecondary.invalidateAll()
                  }

                  retryLoop(retries + 1)
                case other =>
                  val error = resp.value[GenericSoQLError[MT#ResourceNameScope]]() match {
                    case Right(value) => value
                    case Left(err) => throw new Exception("Invalid error response: " + err.english)
                  }
                  base ~> JsonResp(error)
              }
            case Left(errorResponse) =>
              errorResponse
          }
        }

        retryLoop(0)
      case Left(err) =>
        BadRequest ~> JsonResp(err)
    }
  }

  override val query: HttpRequest => HttpResponse = handle(_)(doit _)
}

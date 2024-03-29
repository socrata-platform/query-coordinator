package com.socrata.querycoordinator.resources

import scala.concurrent.duration._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.ast.{JString, JValue, JNull}
import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode, DecodeError}
import com.rojoma.json.v3.util.{WrapperJsonCodec, AutomaticJsonCodec, NullForNone, AllowMissing, SimpleHierarchyCodecBuilder, InternalTag, AutomaticJsonCodecBuilder, JsonUtil}
import com.rojoma.simplearm.v2.ResourceScope
import org.apache.commons.io.HexDump
import org.slf4j.LoggerFactory
import org.joda.time.{DateTime, LocalDateTime}

import com.socrata.http.client.HttpClient
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
import com.socrata.soql.stdlib.analyzer2.Context
import com.socrata.soql.sql.Debug
import com.socrata.soql.util.GenericSoQLError

import com.socrata.querycoordinator.Secondary
import com.socrata.querycoordinator.util.Lazy

class NewQueryResource(
  httpClient: HttpClient,
  secondary: Secondary
) extends QCResource with ResourceExt {
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

  val standardSystemColumns = Set(":id", ":version", ":created_at", ":updated_at").map(ColumnName)

  val analyzer = new SoQLAnalyzer[MT](SoQLTypeInfo.soqlTypeInfo2, SoQLFunctionInfo, MT.ToProvenance)
  val systemColumnPreservingAnalyzer = analyzer
    .preserveSystemColumns { (columnName, expr) =>
      if(standardSystemColumns(columnName)) {
        Some(AggregateFunctionCall[MT](
          MonomorphicFunction(SoQLFunctions.Max, Map("a" -> expr.typ)),
          Seq(expr),
          false,
          None
        )(FuncallPositionInfo.Synthetic))
      } else {
        None
      }
    }

  implicit val analyzerErrorCodecs =
    SoQLAnalyzerError.errorCodecs[MT#ResourceNameScope, SoQLAnalyzerError[MT#ResourceNameScope]]().
      build

  private case class Request(
    analysis: SoQLAnalysis[MT],
    locationSubcolumns: Map[types.DatabaseTableName[MT], Map[types.DatabaseColumnName[MT], Seq[Option[types.DatabaseColumnName[MT]]]]],
    systemContext: Map[String, String],
    rewritePasses: Seq[Seq[Pass]],
    allowRollups: Boolean,
    debug: Option[Debug],
    queryTimeout: Option[FiniteDuration]
  )
  private object Request {
    implicit def serialize(implicit ev: Writable[SoQLAnalysis[MT]]) = new Writable[Request] {
      def writeTo(buffer: WriteBuffer, req: Request): Unit = {
        buffer.write(1)
        buffer.write(req.analysis)
        buffer.write(req.locationSubcolumns)
        buffer.write(req.systemContext)
        buffer.write(req.rewritePasses)
        buffer.write(req.allowRollups)
        buffer.write(req.debug)
        buffer.write(req.queryTimeout.map(_.toMillis))
      }
    }
  }

  @AutomaticJsonCodec
  case class Body(
    foundTables: FoundTables[MT],
    @AllowMissing("Nil")
    locationSubcolumns: Seq[(types.DatabaseTableName[MT], Seq[(types.DatabaseColumnName[MT], Seq[Either[JNull, types.DatabaseColumnName[MT]]])])],
    @AllowMissing("Context.empty")
    context: Context,
    @AllowMissing("Nil")
    rewritePasses: Seq[Seq[Pass]],
    @AllowMissing("true")
    allowRollups: Boolean,
    @AllowMissing("false")
    preserveSystemColumns: Boolean,
    debug: Option[Debug],
    queryTimeoutMS: Option[Long],
    store: Option[String]
  )

  def doit(rs: ResourceScope, headers: HeaderMap, reqId: RequestId, json: Json[Body]): HttpResponse = {
    val Json(
      reqData@Body(
        foundTables,
        locationSubcolumns,
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

    val mapifiedLocationSubcolumns = locationSubcolumns.iterator.map { case (dtn, submap) =>
      dtn -> submap.iterator.map { case (dcn, cols) =>
        dcn -> cols.map {
          case Left(JNull) => None
          case Right(dcn2) => Some(dcn2)
        }
      }.toMap
    }.toMap

    val effectiveAnalyzer =
      if(preserveSystemColumns) systemColumnPreservingAnalyzer
      else analyzer

    effectiveAnalyzer(foundTables, context.user.toUserParameters) match {
      case Right(analysis) =>
        val serialized = WriteBuffer.asBytes(Request(analysis, mapifiedLocationSubcolumns, context.system, rewritePasses, allowRollups, debug, queryTimeoutMS.map(_.milliseconds)))

        log.debug("Serialized analysis as:\n{}", Lazy(locally {
          val baos = new ByteArrayOutputStream
          HexDump.dump(serialized, 0, baos, 0)
          new String(baos.toByteArray, StandardCharsets.ISO_8859_1)
        }))

        var allSecondaries: Stream[Option[String]] =
          store match {
            case None =>
              analysis.statement.allTables.toStream.scanLeft((Option.empty[String], Set.empty[String])) { (acc, dss) =>
                val (_, alreadySeen) = acc
                val DatabaseTableName((DatasetInternalName(n), Stage(s))) = dss
                secondary.chosenSecondaryName(None, n, Some(s), alreadySeen) match {
                  case Some(secondary) => (Some(secondary), alreadySeen + secondary)
                  case None => (None, alreadySeen)
                }
              }.map(_._1).tail
            case Some(store) =>
              log.info("Forcing secondary to {}", JString(store))
              Stream(Some(store))
          }

        // ok so it's possible that allTables is empty, in which case
        // we don't care what secondary we're sending this to.
        if(allSecondaries.isEmpty) {
          allSecondaries = Stream(Some(secondary.arbitrarySecondary()))
        }

        val additionalHeaders = Seq("if-none-match", "if-match", "if-modified-since").flatMap { h =>
          headers.lookup(HeaderName(h)).map { v => h -> v.underlying }
        }

        for {
          chosenSecondary <- allSecondaries.takeWhile(_.isDefined).map(_.get)
          instance <- secondary.unassociatedServiceInstance(chosenSecondary)
        } {
          val req = secondary.reqBuilder(instance)
            .p("new-query")
            .addHeaders(additionalHeaders)
            .addHeader(ReqIdHeader, reqId.toString)
            .blob(new ByteArrayInputStream(serialized))
          val resp = httpClient.execute(req, rs)

          val base = Status(resp.resultCode) ~> Header("x-soda2-secondary", chosenSecondary)

          return resp.resultCode match {
            case code@(200 | 304) =>
              Seq("content-type", "etag", "last-modified").foldLeft[HttpResponse](base) { (acc, hdrName) =>
                resp.headers(hdrName).foldLeft(acc) { (acc, value) =>
                  acc ~> Header(hdrName, value)
                }
              } ~> StreamBytes(resp.inputStream())

            case other =>
              val error = resp.value[GenericSoQLError[MT#ResourceNameScope]]() match {
                case Right(value) => value
                case Left(err) => throw new Exception("Invalid error response: " + err.english)
              }
              base ~> JsonResp(error)
          }
        }

        throw new Exception("Failed to find a secondary") // TODO
      case Left(err) =>
        BadRequest ~> JsonResp(err)
    }
  }

  override val query: HttpRequest => HttpResponse = handle(_)(doit _)
}

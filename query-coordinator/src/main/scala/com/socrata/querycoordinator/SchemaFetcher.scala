package com.socrata.querycoordinator

import java.io.IOException
import javax.servlet.http.HttpServletResponse

import com.socrata.http.client.exceptions._
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.common.util.HttpUtils
import com.socrata.querycoordinator.SchemaFetcher._
import org.joda.time.DateTime

trait SchemaFetcher {

  def apply(base: RequestBuilder, dataset: String, copy: Option[String], useResourceName: Boolean = false): Result
}

class HttpSchemaFetcher(httpClient: HttpClient) extends SchemaFetcher {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SchemaFetcher])

  def apply(base: RequestBuilder, dataset: String, copy: Option[String], useResourceName: Boolean = false): Result = {
    def processResponse(response: Response): Result = response.resultCode match {
      case HttpServletResponse.SC_OK =>
        val dataVersion = response.headers("X-SODA2-DataVersion")(0).toLong
        val copyNumber = response.headers("X-SODA2-CopyNumber")(0).toLong
        val lastModified = HttpUtils.parseHttpDate(response.headers("Last-Modified")(0))
        response.value[SchemaWithFieldName]() match {
          case Right(schema) =>
            Successful(schema, copyNumber, dataVersion, lastModified)
          case Left(err) =>
            log.warn("cannot decode schema {}", err)
            NonSchemaResponse
        }
      case HttpServletResponse.SC_NOT_FOUND =>
        NoSuchDatasetInSecondary
      case other: Int =>
        log.error("Unexpected response code {} from request for schema of dataset {} from {}:{}",
          other.asInstanceOf[AnyRef], dataset.asInstanceOf[AnyRef], base.url)
        BadResponseFromSecondary
    }

    val idParam = if (useResourceName) "rn" else "ds"
    val params = Seq(idParam -> dataset, "fieldName" -> true.toString) ++ copy.map(c => Seq("copy" -> c)).getOrElse(Nil)
    val request = base.p("schema").q(params: _*).get

    try {
      httpClient.execute(request).run(processResponse)
    } catch {
      case e: ConnectTimeout =>
        SecondaryConnectFailed
      case e: ConnectFailed =>
        SecondaryConnectFailed
      case e: HttpClientTimeoutException =>
        TimeoutFromSecondary
      case e: LivenessCheckFailed =>
        TimeoutFromSecondary
      case e: HttpClientException =>
        BadResponseFromSecondary
      case e: IOException =>
        BadResponseFromSecondary
    }
  }
}

object SchemaFetcher {

  def apply(httpClient: HttpClient): HttpSchemaFetcher = new HttpSchemaFetcher(httpClient)

  sealed abstract class Result

  case class Successful(schema: SchemaWithFieldName, copyNumber: Long, dataVersion: Long, lastModified: DateTime) extends Result

  case object NonSchemaResponse extends Result

  case object NoSuchDatasetInSecondary extends Result

  case object BadResponseFromSecondary extends Result

  case object TimeoutFromSecondary extends Result

  case object SecondaryConnectFailed extends Result
}

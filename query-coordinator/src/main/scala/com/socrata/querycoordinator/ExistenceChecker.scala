package com.socrata.querycoordinator

import java.io.IOException
import javax.servlet.http.HttpServletResponse

import com.socrata.http.client.exceptions._
import com.socrata.http.client.{HttpClient, RequestBuilder, Response}
import com.socrata.http.common.util.HttpUtils
import com.socrata.querycoordinator.ExistenceChecker._
import org.joda.time.DateTime

trait ExistenceChecker {

  def apply(base: RequestBuilder, dataset: String, copy: Option[String]): Result
}

class HttpExistenceChecker(httpClient: HttpClient) extends ExistenceChecker {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[ExistenceChecker])

  def apply(base: RequestBuilder, dataset: String, copy: Option[String]): Result = {
    def processResponse(response: Response): Result = response.resultCode match {
      case HttpServletResponse.SC_OK => Yes
      case HttpServletResponse.SC_NOT_FOUND => No
      case other: Int =>
        log.error("Unexpected response code {} from request for existence of dataset {} from {}:{}",
          other.asInstanceOf[AnyRef], dataset.asInstanceOf[AnyRef], base.url)
        BadResponseFromSecondary
    }

    val params = Seq("ds" -> dataset) ++ copy.map(c => Seq("copy" -> c)).getOrElse(Nil)
    val request = base.p("exists").q(params: _*).get

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

object ExistenceChecker {

  def apply(httpClient: HttpClient) = new HttpExistenceChecker(httpClient)

  sealed abstract class Result

  case object Yes extends Result

  case object No extends Result

  sealed abstract class Error extends Result

  case object BadResponseFromSecondary extends Error

  case object TimeoutFromSecondary extends Error

  case object SecondaryConnectFailed extends Error
}

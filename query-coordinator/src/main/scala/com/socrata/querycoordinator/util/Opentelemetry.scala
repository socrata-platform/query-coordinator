package com.socrata.querycoordinator.util

import scala.collection.JavaConverters._

import java.io.Closeable

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{Tracer, SpanKind, Span}
import io.opentelemetry.context.propagation.{ContextPropagators, TextMapSetter}
import io.opentelemetry.context.Context
import io.opentelemetry.api.incubator.trace.ExtendedSpanBuilder
import io.opentelemetry.api.incubator.propagation.ExtendedContextPropagators
import com.rojoma.simplearm.v2._
import com.socrata.http.client.{HttpClient, RequestBuilder, SimpleHttpRequest, BodylessHttpRequest, FormHttpRequest, FileHttpRequest, JsonHttpRequest, BlobHttpRequest}
import com.socrata.http.server.{HttpRequest, HttpService}


object OpenTelemetryUtils {
  class OtelHandler(underlying: HttpService, tracer: Tracer, propagators: ContextPropagators) extends HttpService {
    def apply(req: HttpRequest) = { resp =>
      tracer.spanBuilder(s"${req.method} ${req.requestPathStr}")
        .asInstanceOf[ExtendedSpanBuilder]
        .setParentFrom(propagators, req.servletRequest.getHeaderNames.asScala.map { h => h -> req.header(h).get }.toMap.asJava)
        .setSpanKind(SpanKind.SERVER)
        .startAndRun { () =>
          val span = Span.current();
          span.setAttribute("component", "http");
          span.setAttribute("http.method", req.method);
          span.setAttribute("http.scheme", "http");
          span.setAttribute("http.target", req.requestPathStr);
          underlying(req)(resp)
        }
    }
  }

  object OtelHandler {
    def apply(tracer: Tracer, propagators: ContextPropagators)(handler: HttpService) =
      new OtelHandler(handler, tracer, propagators)
  }

  class OtelHttpClient(underlying: HttpClient, otel: OpenTelemetry) extends HttpClient {
    override def close() = underlying.close()

    override def executeRawUnmanaged(req: SimpleHttpRequest): RawResponse with Closeable = {
      new RawResponse with Closeable {
        val resp = underlying.executeRawUnmanaged(augment(req))

        override def close() = resp.close()
        override val body = resp.body
        override val responseInfo = resp.responseInfo
      }
    }

    private def augment(req: SimpleHttpRequest): SimpleHttpRequest = {
      req match {
        case bhr: BodylessHttpRequest =>
          new BodylessHttpRequest(addHeaders(bhr.builder))
        case fhr: FormHttpRequest =>
          new FormHttpRequest(addHeaders(fhr.builder), fhr.contents)
        case fhr: FileHttpRequest =>
          new FileHttpRequest(addHeaders(fhr.builder), fhr.contents, fhr.file, fhr.field, fhr.contentType)
        case jhr: JsonHttpRequest =>
          new JsonHttpRequest(addHeaders(jhr.builder), jhr.contents)
        case bhr: BlobHttpRequest =>
          new BlobHttpRequest(addHeaders(bhr.builder), bhr.contents, bhr.contentType)
      }
    }

    private def addHeaders(req: RequestBuilder): RequestBuilder = {
      val headers = Vector.newBuilder[(String, String)]
      ExtendedContextPropagators.getTextMapPropagationContext(otel.getPropagators)
        .forEach { (k, v) => headers += k -> v }
      req.addHeaders(headers.result())
    }
  }
}

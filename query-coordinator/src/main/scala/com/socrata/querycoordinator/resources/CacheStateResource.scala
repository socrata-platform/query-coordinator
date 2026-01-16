package com.socrata.querycoordinator.resources

import com.rojoma.json.v3.ast.JValue

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.{HttpRequest, HttpResponse}

class CacheStateResource(getState: () => JValue) extends QCResource {
  override val get: HttpRequest => HttpResponse = { req =>
    OK ~> Json(getState())
  }
}

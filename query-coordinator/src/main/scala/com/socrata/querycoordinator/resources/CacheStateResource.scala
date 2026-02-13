package com.socrata.querycoordinator.resources

import com.rojoma.json.v3.ast.JValue

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.querycoordinator.secondary_finder.CachedSecondaryInstanceFinder

class CacheStateResource(cache: CachedSecondaryInstanceFinder[(NewQueryResource.DatasetInternalName, NewQueryResource.Stage), String]) extends QCResource {
  implicit val dinEncode = new com.rojoma.json.v3.codec.FieldEncode[(NewQueryResource.DatasetInternalName, NewQueryResource.Stage)] {
    override def encode(v: (NewQueryResource.DatasetInternalName, NewQueryResource.Stage)) =
      v._1.underlying + "@" + v._2.underlying
  }

  override val get: HttpRequest => HttpResponse = { req =>
    OK ~> Json(cache.toJValue)
  }

  object Prime extends QCResource {
    override val get: HttpRequest => HttpResponse = { req =>
      OK ~> Json(cache.primingData)
    }
  }
}

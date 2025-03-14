package com.socrata.querycoordinator

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import com.socrata.querycoordinator.resources._


class State() {
  var beDead = false
}

object State {
  val state: State = new State()
}

/**
 * Main HTTP resource servicing class
 */
class Service(
  queryResource: QueryResource,
  newQueryResource: NewQueryResource,
  versionResource: VersionResource,
  deathResource: DeathResource
) extends HttpService {

  val log = org.slf4j.LoggerFactory.getLogger(classOf[Service])

  // Little dance because "/*" doesn't compile yet and I haven't
  // decided what its canonical target should be (probably "/query")
  val routingTable = Routes(
    Route("/{String}/+", (_: Any, _: Any) => queryResource),
    Route("/{String}", (_: Any) => queryResource),
    Route("/new-query", newQueryResource),
    Route("/death", deathResource),
    Route("/version", versionResource)
  )

  def apply(req: HttpRequest): HttpResponse ={
    routingTable(req.requestPath) match {
      case Some(resource) => resource(req)
      case None => NotFound
    }
  }

}
object Service {
  def apply(queryResource: QueryResource, newQueryResource: NewQueryResource, versionResource: VersionResource, deathResource: DeathResource): Service = {
    new Service(queryResource, newQueryResource, versionResource, deathResource)
  }

}

package com.socrata.querycoordinator.resources

import com.rojoma.json.v3.ast.{JString, JObject}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.querycoordinator.BuildInfo
import com.socrata.http.server.implicits._
import org.slf4j.LoggerFactory

class DeathResource extends QCResource {
  val log = LoggerFactory.getLogger(classOf[DeathResource])


  override val get: HttpService = {
    _: HttpRequest => {
      log.info(s"you've asked me to die")
      val state = com.socrata.querycoordinator.State.state
      log.info(s"old state is $state")
      state.beDead = !state.beDead
      log.info(s"new state is $state")
      OK ~> Content("text/plain", s"The new state is ${state.beDead}\n")
    }
  }

}
object DeathResource {
  def apply(): DeathResource = {
    new DeathResource()
  }
}

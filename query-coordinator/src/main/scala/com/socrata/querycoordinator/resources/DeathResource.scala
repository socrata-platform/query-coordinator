package com.socrata.querycoordinator.resources

import com.rojoma.json.v3.ast.{JString, JObject}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.querycoordinator.BuildInfo
import com.socrata.http.server.implicits._


class DeathResource extends QCResource {

  override val get: HttpService = {
    _: HttpRequest => {
      val state = com.socrata.querycoordinator.State.state
      println(s"old state is $state")
      state.beDead = !state.beDead
      println(s"new state is $state")
      OK ~> Content("application/text", s"The new state is ${state.beDead}\n")
    }
  }

}
object DeathResource {
  def apply(): DeathResource = {
    new DeathResource()
  }
}

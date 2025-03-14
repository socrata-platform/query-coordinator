package com.socrata.querycoordinator.resources

import com.rojoma.json.v3.ast.{JString, JObject}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.http.server._
import com.socrata.http.server.responses._
import com.socrata.querycoordinator.BuildInfo
import com.socrata.http.server.implicits._
import org.slf4j.LoggerFactory


class VersionResource extends QCResource {
  val log = LoggerFactory.getLogger(classOf[DeathResource])


  val version = JsonUtil.renderJson(JObject(BuildInfo.toMap.mapValues(v => JString(v.toString))))

  override val get: HttpService = {
    _: HttpRequest => {
      val state = com.socrata.querycoordinator.State.state
      log.info(s"I am ${state.beDead}")
      while (state.beDead) {
        Thread.sleep(2000)
        log.info("i am dead")
      }
      OK ~> Content("application/json", version)
    }
  }

}
object VersionResource {
  def apply(): VersionResource = {
    new VersionResource()
  }
}

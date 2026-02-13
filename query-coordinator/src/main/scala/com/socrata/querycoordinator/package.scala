package com.socrata

import org.apache.curator.x.discovery.ServiceInstance

import com.socrata.http.client.RequestBuilder
import com.socrata.http.common.AuxiliaryData

package object querycoordinator {
  implicit class ServiceInstanceExt(private val serviceInstance: ServiceInstance[AuxiliaryData]) extends AnyVal {
    def toRequestBuilder: RequestBuilder = {
      val pingTarget = for {
        auxData <- Option(serviceInstance.getPayload)
        pingInfo <- auxData.livenessCheckInfo
      } yield pingInfo

      val rb = RequestBuilder(serviceInstance.getAddress).livenessCheckInfo(pingTarget)

      if(Option(serviceInstance.getSslPort).nonEmpty) {
        rb.secure(true).port(serviceInstance.getSslPort)
      } else if (Option(serviceInstance.getPort).nonEmpty) {
        rb.port(serviceInstance.getPort)
      } else {
        rb
      }
    }
  }
}

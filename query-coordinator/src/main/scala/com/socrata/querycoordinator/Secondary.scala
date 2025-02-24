package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder
import com.socrata.http.common.AuxiliaryData
import com.socrata.querycoordinator.resources.QueryService
import org.apache.curator.x.discovery.ServiceInstance


import scala.concurrent.duration.FiniteDuration

class Secondary(
  secondaryProvider: ServiceProviderProvider[AuxiliaryData],
                existenceChecker: ExistenceChecker,
                secondaryInstance: SecondaryInstanceSelector,
                connectTimeout: FiniteDuration,
                schemaTimeout: FiniteDuration) extends QueryService{

  override val log = org.slf4j.LoggerFactory.getLogger(classOf[Secondary])
  val connectTimeoutMillis = connectTimeout.toMillis.toInt
  val schemaTimeoutMillis = schemaTimeout.toMillis.toInt


  def chosenSecondaryName(forcedSecondaryName: Option[String],
                          dataset: String, copy: Option[String],
                          excludedSecondaryNames: Set[String]): Option[String] = {
    forcedSecondaryName.orElse {
      secondaryInstance.getInstanceName(dataset, isInSecondary(_, dataset, copy), excludedSecondaryNames)
    }
  }

  def arbitrarySecondary(): String = {
    secondaryInstance.arbitraryInstance()
  }

  def allSecondaries(): Seq[String] = {
    secondaryInstance.allInstances()
  }

  def unassociatedServiceInstance(instanceName: String): Option[ServiceInstance[AuxiliaryData]] =
    Option(secondaryProvider.provider(instanceName).getInstance())

  def serviceInstance(dataset: String, instanceName: Option[String], markBrokenOnUnknown: Boolean = true): Option[ServiceInstance[AuxiliaryData]] = {
    val instance = for {
      name <- instanceName
      instance <- Option(secondaryProvider.provider(name).getInstance())
    } yield instance

    if (markBrokenOnUnknown && instance.isEmpty) {
      instanceName.foreach { n => secondaryInstance.flagError(dataset, n) }
    }

    instance
  }

  def isInSecondary(name: String, dataset: String, copy: Option[String]): Option[Boolean] = {
    for {
      instance <- Option(secondaryProvider.provider(name).getInstance())
      base <- Some(reqBuilder(instance))
      result <- existenceChecker(base.receiveTimeoutMS(schemaTimeoutMillis), dataset, copy) match {
        case ExistenceChecker.Yes => Some(true)
        case ExistenceChecker.No => Some(false)
        case other: ExistenceChecker.Result =>
          log.warn(unexpectedError, other)
          None
      }
    } yield result
  }


  def reqBuilder(secondary: ServiceInstance[AuxiliaryData]): RequestBuilder = {
    val pingTarget = for {
      auxData <- Option(secondary.getPayload)
      pingInfo <- auxData.livenessCheckInfo
    } yield pingInfo

    val rb = RequestBuilder(secondary.getAddress).livenessCheckInfo(pingTarget).connectTimeoutMS(connectTimeoutMillis)

    if(Option(secondary.getSslPort).nonEmpty) {
      rb.secure(true).port(secondary.getSslPort)
    } else if (Option(secondary.getPort).nonEmpty) {
      rb.port(secondary.getPort)
    } else {
      rb
    }
  }

}
object Secondary {
  def apply(secondaryProvider: ServiceProviderProvider[AuxiliaryData],
            existenceChecker: ExistenceChecker,
            secondaryInstance: SecondaryInstanceSelector,
            connectTimeout: FiniteDuration,
            schemaTimeout: FiniteDuration): Secondary = {

    new Secondary(secondaryProvider, existenceChecker, secondaryInstance, connectTimeout, schemaTimeout)
  }

}

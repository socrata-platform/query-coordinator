package com.socrata.querycoordinator

case class Mirror(mirrors: Map[String, List[String]]) {
  def secondaryMirrors(secondaryName: String): Set[String] =
    mirrors.filterKeys(secondaryName.contains).values.flatten.toSet
}

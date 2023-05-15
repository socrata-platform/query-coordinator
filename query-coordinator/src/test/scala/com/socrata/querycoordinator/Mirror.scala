package com.socrata.querycoordinator

import org.scalatest.FunSuite
import org.scalatest.Matchers

class TestMirror extends FunSuite with Matchers {
  test("Selecting item that does not exist yields empty list") {
    Mirror(Map.empty).secondaryMirrors("foo") shouldEqual Set()
  }

  test("Selecting item with one mirror yields one mirror") {
    Mirror(Map("foo" -> List("foo1", "foo2"))).secondaryMirrors("foo") shouldEqual Set("foo1", "foo2")
  }

  test("Selecting item with more than one mirror yields more than one mirror") {
    Mirror(Map("foo" -> List("bar1", "bar2"), "bar" -> List("foo1", "foo2")))
      .secondaryMirrors("foobarbaz") shouldEqual Set("foo1", "foo2", "bar1", "bar2")
  }
}

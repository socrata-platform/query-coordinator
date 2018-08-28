package com.socrata.querycoordinator.caching.cache

import java.io.{BufferedWriter, OutputStreamWriter, Writer, OutputStream}
import java.nio.charset.StandardCharsets

import com.rojoma.simplearm.v2._

/**
 * A session is NOT necessarily thread-safe.  It can be used from multiple threads
 * (there is no hidden thread-local state) but not concurrently.
 */
trait CacheSession {
  /** Looks up a key by name.  The returned ValueRef is valid until given
    * `ResourceScope` closes it or this cache session is closed, whichever
    * comes first.
    */
  def find(key: String, resourceScope: ResourceScope): CacheSession.Result[Option[ValueRef]]

  /** Create or replace a key.  It will not be visible until `filler` returns,
    * and MAY not be visible for some time thereafter. */
  def create(key: String)(filler: CacheSession.Result[OutputStream] => Unit): Unit

  final def createText(key: String)(filler: CacheSession.Result[Writer] => Unit) =
    create(key) { osr =>
      val wr = osr.map { os => new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)) }
      try {
        filler(wr)
      } finally {
        wr.foreach(_.close())
      }
    }
}

object CacheSession {
  sealed trait Result[+T] {
    def map[U](f: T => U): Result[U]
    def foreach[U](f: T => U): Unit
  }
  case object Timeout extends Result[Nothing] {
    def map[U](f: Nothing => U) = this
    def foreach[U](f: Nothing => U): Unit = {}
  }
  case class Success[+T](value: T) extends Result[T] {
    def map[U](f: T => U) = Success(f(value))
    def foreach[U](f: T => U): Unit = f(value)
  }
}

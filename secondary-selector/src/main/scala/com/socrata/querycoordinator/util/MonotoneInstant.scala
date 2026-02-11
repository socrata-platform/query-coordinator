package com.socrata.querycoordinator.util

import scala.concurrent.duration.FiniteDuration

import java.time.Instant
import java.util.concurrent.TimeUnit

import com.rojoma.json.v3.ast.JString
import com.rojoma.json.v3.codec.JsonEncode

class MonotoneInstant private (private val nanoTime: Long) extends AnyVal {
  def -(that: MonotoneInstant): FiniteDuration = FiniteDuration(this.nanoTime - that.nanoTime, TimeUnit.NANOSECONDS)
  def +(that: FiniteDuration): MonotoneInstant = new MonotoneInstant(this.nanoTime + that.toNanos)
  def -(that: FiniteDuration): MonotoneInstant = new MonotoneInstant(this.nanoTime - that.toNanos)
  def elapsed = MonotoneInstant.now() - this

  def approxWallClockTime =
    Instant.ofEpochMilli(System.currentTimeMillis - elapsed.toMillis)

  override def toString = approxWallClockTime.toString
}
object MonotoneInstant {
  def now(): MonotoneInstant = new MonotoneInstant(System.nanoTime())

  implicit val jEncode = new JsonEncode[MonotoneInstant] {
    override def encode(i: MonotoneInstant) =
      JString(i.toString)
  }
}


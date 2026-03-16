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
  private val epoch = System.nanoTime()

  private def epochNanos() = System.nanoTime() - epoch

  def now(): MonotoneInstant = new MonotoneInstant(epochNanos())

  def fromWallClockTime(instant: Instant): Option[MonotoneInstant] = {
    try {
      val nowNanos = epochNanos()
      val distanceInPastMillis = Math.subtractExact(Instant.now().toEpochMilli, instant.toEpochMilli)
      val distanceInPastNanos = Math.multiplyExact(distanceInPastMillis, 1000000)
      Some(new MonotoneInstant(Math.subtractExact(nowNanos, distanceInPastNanos)))
    } catch {
      case _ : ArithmeticException =>
        None
    }
  }

  implicit val jEncode = new JsonEncode[MonotoneInstant] {
    override def encode(i: MonotoneInstant) =
      JString(i.toString)
  }
}


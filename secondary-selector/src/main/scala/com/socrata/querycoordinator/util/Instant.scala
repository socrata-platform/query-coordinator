package com.socrata.querycoordinator.util

import scala.concurrent.duration.FiniteDuration

import java.util.concurrent.TimeUnit

class Instant private (private val nanoTime: Long) extends AnyVal {
  def -(that: Instant): FiniteDuration = FiniteDuration(this.nanoTime - that.nanoTime, TimeUnit.NANOSECONDS)
  def +(that: FiniteDuration): Instant = new Instant(this.nanoTime + that.toNanos)
  def -(that: FiniteDuration): Instant = new Instant(this.nanoTime - that.toNanos)
  def elapsed = Instant.now() - this
}
object Instant {
  def now(): Instant = new Instant(System.nanoTime())
}


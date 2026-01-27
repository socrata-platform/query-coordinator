package com.socrata.querycoordinator

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

import java.util.concurrent.{ConcurrentHashMap, TimeUnit, ArrayBlockingQueue, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicReference

import org.slf4j.LoggerFactory

abstract class SecondaryResult[Secondary] {
  val secondary: Secondary

  // Invalidate the result for this dataset on all secondaries,
  // forcing re-checking the next time it's accessed.
  def invalidateAll(): Unit

  // Mark the result on _this_ secondary as Unknown.  Does not force
  // re-checking on all secondaries.  Use this with caution!  It can
  // interact badly with multi-dataset queries if the cache ends up in
  // a state where the datasets are believed to be in a disjoint
  // state.
  def invalidateSecondary(): Unit

  override final def hashCode = secondary.hashCode
  override final def equals(that: Any) =
    that match {
      case s: SecondaryResult[_] => this.secondary == s.secondary
      case _ => false
    }

  override final def toString =
    s"SecondaryResult($secondary)"
}

object SecondaryResult {
  // A pure secondary, without any "dataset" involved
  class Simple[Secondary](val secondary: Secondary) extends SecondaryResult[Secondary] {
    override def invalidateAll() = {}
    override def invalidateSecondary() = {}
  }
}

trait NewSecondaryInstanceSelector[DatasetInternalName, Secondary] {
  val allSecondaries: Set[SecondaryResult[Secondary]]

  def invalidate(internalNames: Iterable[DatasetInternalName]): Unit

  // Find a secondary containing _all_ the given datasets
  def whereAre(internalNames: Iterable[DatasetInternalName]): Set[SecondaryResult[Secondary]]

  final def whereIs(internalName: DatasetInternalName): Set[SecondaryResult[Secondary]] =
    whereAre(internalName :: Nil)
}

sealed abstract class CheckResult
object CheckResult {
  case object Present extends CheckResult
  case object Absent extends CheckResult
  case object Unknown extends CheckResult
}

class Instant private (private val nanoTime: Long) extends AnyVal {
  def -(that: Instant): FiniteDuration = FiniteDuration(this.nanoTime - that.nanoTime, TimeUnit.NANOSECONDS)
  def -(that: FiniteDuration): Instant = new Instant(this.nanoTime - that.toNanos)
  def elapsed = Instant.now() - this
}
object Instant {
  def now(): Instant = new Instant(System.nanoTime())
}

// The way this cache works is:
//    "Yes, I have it" results are cached until explicitly invalidated.
//    At most once every absentInterval, "Absent" results may be re-checked.
//    At once every unknownInterval, "Unknown" results may be re-checked.
// When a req comes in for a dataset with a sufficiently-old absent or unknown
// response, there is a 1-in-{associated odds} request it will be chosen to
// be re-checked.  When this happens the timestamp on it is reset.  At most
// one secondary will be scheduled for rechecking per request.
class NewSecondaryInstanceSelectorImpl[DatasetInternalName, Secondary](
  allSecondariesRaw: Set[Secondary],
  check: (Secondary, DatasetInternalName) => CheckResult,
  absentInterval: FiniteDuration,
  absentOdds: Int,
  unknownInterval: FiniteDuration,
  unknownOdds: Int,
) extends NewSecondaryInstanceSelector[DatasetInternalName, Secondary] {
  private val log = LoggerFactory.getLogger(classOf[NewSecondaryInstanceSelectorImpl[_, _]])

  private val allSecondariesSeq = allSecondariesRaw.toSeq
  override val allSecondaries = allSecondariesRaw.map(new SecondaryResult.Simple(_))

  private val rechecker = new Worker

  def start() {
    rechecker.start()
  }

  def stop() {
    rechecker.done = true
    rechecker.queue.offer(Task.Stop) // doesn't matter if this successfully enqueues; if it doesn't, there's pending work for the checker to see
    rechecker.join()
  }

  private sealed abstract class CacheResult {
    val checkedAt: Instant
  }
  private object CacheResult {
    case class Present(checkedAt: Instant) extends CacheResult
    case class Absent(checkedAt: Instant) extends CacheResult
    case class Unknown(checkedAt: Instant) extends CacheResult

    def fromCheckResult(checkResult: CheckResult): CacheResult = {
      checkResult match {
        case CheckResult.Present => Present(Instant.now())
        case CheckResult.Absent => Absent(Instant.now())
        case CheckResult.Unknown => Unknown(Instant.now())
      }
    }
  }

  private sealed abstract class Cached
  private object Cached {
    // these are deliberately not case classes.  They have identity
    // semantics, and Pending requires holding a lock to access the
    // result field.
    class Pending(var result: Option[Completed]) extends Cached
    class Completed(val result: Map[Secondary, AtomicReference[CacheResult]]) extends Cached
  }
  private val cache = new ConcurrentHashMap[DatasetInternalName, Cached]

  private class SR(
    val internalNames: Map[DatasetInternalName, (Cached.Completed, CacheResult.Present)],
    override val secondary: Secondary
  ) extends SecondaryResult[Secondary] {
    override def invalidateAll(): Unit = {
      for((internalName, (origCompleted, _)) <- internalNames) {
        cache.remove(internalName, origCompleted)
      }
    }

    override def invalidateSecondary(): Unit = {
      for((internalName, (_, origPresent)) <- internalNames) {
        Option(cache.get(internalName)).foreach {
          case completed: Cached.Completed =>
            completed.result.get(secondary).foreach { v =>
              v.compareAndSet(origPresent, CacheResult.Unknown(Instant.now() - unknownInterval))
            }
          case _ =>
            // Not there to be invalidated, so we're done
        }
      }
    }
  }

  private implicit class IteratorExt[T](private val it: Iterator[T]) {
    def nextOption(): Option[T] = {
      if(it.hasNext) {
        Some(it.next())
      } else {
        None
      }
    }
  }

  private implicit class BufferedIteratorExt[T](private val it: BufferedIterator[T]) {
    def headOption: Option[T] = {
      if(it.hasNext) {
        Some(it.head)
      } else {
        None
      }
    }
  }

  private def upcast[A, B >: A](s: Set[A]): Set[B] = s.asInstanceOf[Set[B]]

  override def invalidate(internalNames: Iterable[DatasetInternalName]): Unit = {
    for(internalName <- internalNames) {
      cache.remove(internalName)
    }
  }

  override def whereAre(internalNames: Iterable[DatasetInternalName]): Set[SecondaryResult[Secondary]] = {
    val it = internalNames.iterator.buffered
    it.nextOption() match {
      case None =>
        Set.empty
      case Some(hd) =>
        it.headOption match {
          case None =>
            // common case: only one dataset in the query
            upcast(whereis1(hd))
          case Some(_) =>
            // Ok, so we're looking for a set of secondaries that
            // _all_ the datasets are in.  So what we'll do is find
            // the set of secondaries for each, and then
            // intersect-merge them.  To do this, we'll build a map
            // of secondaryName -> SR
            val merged = it.map { dsin =>
              val secondaries = whereis1(dsin)
              secondaries.iterator.map { s => s.secondary -> s }.toMap
            }.foldLeft(whereis1(hd).iterator.map { s => s.secondary -> s }.toMap) { (acc: Map[Secondary, SR], mergeSet: Map[Secondary, SR]) =>
              val commonSecondaries = acc.keySet.intersect(mergeSet.keySet)
              commonSecondaries.iterator.map { commonSecondary =>
                commonSecondary -> new SR(acc(commonSecondary).internalNames ++ mergeSet(commonSecondary).internalNames, commonSecondary)
              }.toMap
            }
            upcast(merged.valuesIterator.toSet)
        }
    }
  }

  private def whereis1(internalName: DatasetInternalName): Set[SR] = {
    val WhereisResult(completed, byMe) = doWhereis(internalName)

    var recheckedOne = false
    val now = Instant.now()
    val rng = new Random(ThreadLocalRandom.current())
    val result = rng.shuffle(completed.result.toSeq)
      .iterator
      .flatMap { case (secondary, v) =>
        v.get match {
          case present@CacheResult.Present(_) =>
            Some(new SR(Map(internalName -> (completed, present)), secondary))
          case absent@CacheResult.Absent(at) if !byMe && !recheckedOne && now - at > absentInterval && rng.nextInt(absentOdds) == 0 =>
            val newAbsent = CacheResult.Absent(Instant.now())
            if(v.compareAndSet(absent, newAbsent)) {
              recheckedOne = true
              if(!scheduleRecheck(secondary, internalName)) {
                // wasn't able to schedule it
                v.compareAndSet(newAbsent, absent)
                recheckedOne = false
              }
            }
            None
          case unknown@CacheResult.Unknown(at) if !byMe && !recheckedOne && now - at > unknownInterval && rng.nextInt(unknownOdds) == 0 =>
            val newUnknown = CacheResult.Unknown(Instant.now())
            if(v.compareAndSet(unknown, newUnknown)) {
              recheckedOne = true
              if(!scheduleRecheck(secondary, internalName)) {
                // wasn't able to schedule it
                v.compareAndSet(newUnknown, unknown)
                recheckedOne = false
              }
            }
            None
          case _ =>
            None
        }
      }
      .toSet

    if(result.nonEmpty || byMe) { // if I just did the lookup myself, accept returning an empty result
      result
    } else {
      // there were none available - panic!  Well, not really.

      // First, un-cache this name if it hasn't already been replaced by some other thread.
      cache.remove(internalName, completed)

      // If we _still_ don't have any, well.. guess we're returning an
      // empty set at this point.
      val newCompleted = doWhereis(internalName).completed
      newCompleted.result
        .iterator
        .flatMap { case (secondary, v) =>
          v.get match {
            case present@CacheResult.Present(_) =>
              Some(new SR(Map(internalName -> (completed, present)), secondary))
            case _ =>
              None
          }
        }
        .toSet
    }
  }

  private def doCheck(name: DatasetInternalName): Map[Secondary, AtomicReference[CacheResult]] = {
    allSecondariesSeq.par
      .map { secondary =>
        val result: CacheResult = CacheResult.fromCheckResult(check(secondary, name))
        (secondary, new AtomicReference(result))
      }
      .seq
      .toMap
  }

  private case class WhereisResult(completed: Cached.Completed, byMe: Boolean)
  private def doWhereis(internalName: DatasetInternalName): WhereisResult = {
    val pending = new Cached.Pending(None)
    pending.synchronized {
      @tailrec
      def loop(): WhereisResult = {
        cache.computeIfAbsent(internalName, (_) => pending) match {
          case p: Cached.Pending if p eq pending =>
            // I am doing this check
            try {
              val result = new Cached.Completed(doCheck(internalName))
              cache.put(internalName, result)
              pending.result = Some(result)
              WhereisResult(result, byMe = true)
            } finally {
              // if we aren't returning normally, remove our pending
              // object so someone else can have a go.
              cache.remove(internalName, pending)
            }
          case otherPending: Cached.Pending =>
            // someone else is doing the check, wait for their result
            otherPending.synchronized { otherPending.result } match {
              case Some(result) => WhereisResult(result, byMe = false)
              case None => loop() // they didn't finish normally, so try again ourselves
            }
          case result: Cached.Completed =>
            // there was already a cached value, return it
            WhereisResult(result, byMe = false)
        }
      }
      loop()
    }
  }

  private sealed abstract class Task
  private object Task {
    case class Job(secondary: Secondary, internalName: DatasetInternalName) extends Task
    case object Stop extends Task
  }

  private def scheduleRecheck(secondary: Secondary, internalName: DatasetInternalName): Boolean = {
    rechecker.queue.offer(Task.Job(secondary, internalName))
  }

  private class Worker extends Thread {
    setName("Rechecker worker")
    setDaemon(true)

    @volatile var done = false;
    val queue = new ArrayBlockingQueue[Task](100)

    override def run() {
      while(!done) {
        queue.take() match {
          case Task.Job(secondary, internalName) =>
            try {
              Option(cache.get(internalName)).foreach { ent =>
                ent match {
                  case completed: Cached.Completed =>
                    for(value <- completed.result.get(secondary)) {
                      log.info("Re-checking {}:{}", secondary:Any, internalName)
                      val result = CacheResult.fromCheckResult(check(secondary, internalName))
                      value.set(result)
                    }
                  case _ : Cached.Pending =>
                    // nothing to do
                }
              }
            } catch {
              case e: Exception =>
                log.error("Uncaught exception in the rechecker thread", e)
            }
          case Task.Stop =>
            done = true
        }
      }
    }
  }
}

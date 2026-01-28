package com.socrata.querycoordinator.secondary_selector

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

import java.util.concurrent.{ConcurrentHashMap, ArrayBlockingQueue, ThreadLocalRandom}
import java.util.concurrent.atomic.AtomicReference

import org.slf4j.LoggerFactory

import com.socrata.querycoordinator.util.{IteratorExt, Instant}

sealed abstract class CheckResult
object CheckResult {
  case object Present extends CheckResult
  case object Absent extends CheckResult
  case object Unknown extends CheckResult
}

// The way this cache works is:
//    "Yes, I have it" results are cached until explicitly invalidated.
//    At most once every absentInterval, "Absent" results may be re-checked.
//    At once every unknownInterval, "Unknown" results may be re-checked.
// When a req comes in for a dataset with at least one
// sufficiently-old absent or unknown response, one of them it will be
// chosen to be re-checked.  When this happens the timestamp on it is
// reset.
class CachedSecondaryInstanceFinder[DatasetInternalName, Secondary](
  allSecondariesRaw: Set[Secondary],
  check: (Secondary, DatasetInternalName) => CheckResult,
  absentInterval: FiniteDuration,
  absentOdds: Int,
  unknownInterval: FiniteDuration,
  unknownOdds: Int,
) extends SecondaryInstanceFinder[DatasetInternalName, Secondary] {
  private val log = LoggerFactory.getLogger(classOf[CachedSecondaryInstanceFinder[_, _]])

  private val allSecondariesSeq = allSecondariesRaw.toSeq
  override val allSecondaries = allSecondariesRaw.map(new FoundSecondary.Simple(_))

  private val recheckers: Seq[Worker] = (1 to 20).map { _ => new Worker }
  @volatile private var done = false;
  private val recheckerQueue = new ArrayBlockingQueue[Task](100)

  def start() {
    recheckers.foreach(_.start())
  }

  def stop() {
    done = true
    recheckerQueue.offer(Task.Stop) // doesn't matter if this successfully enqueues; if it doesn't, there's pending work for the checker to see
    recheckers.foreach(_.join())
  }

  private sealed abstract class CacheResult {
    val checkedAt: Instant
  }
  private object CacheResult {
    case class Present(checkedAt: Instant) extends CacheResult
    case class Absent(checkedAt: Instant, checkAfter: FiniteDuration) extends CacheResult
    case class Rechecking(checkedAt: Instant) extends CacheResult

    def fromCheckResult(checkResult: CheckResult): CacheResult = {
      checkResult match {
        case CheckResult.Present => Present(Instant.now())
        case CheckResult.Absent => absent(Instant.now())
        case CheckResult.Unknown => unknown(Instant.now())
      }
    }

    def absent(checkedAt: Instant) = Absent(checkedAt, absentInterval)
    def unknown(checkedAt: Instant) = Absent(checkedAt, unknownInterval)
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
  ) extends FoundSecondary[Secondary] {
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
              v.compareAndSet(origPresent, CacheResult.unknown(Instant.now() - unknownInterval))
            }
          case _ =>
            // Not there to be invalidated, so we're done
        }
      }
    }
  }

  // Set, unlike all other immutable collections, is not covariant in
  // its element type because it implements A => Boolean, which
  // requires contravariance.  But in this particular case, that's
  // fine, so just use asInstanceOf to upcast it.
  private def upcast[A, B >: A](s: Set[A]): Set[B] = s.asInstanceOf[Set[B]]

  override def invalidate(internalNames: Iterable[DatasetInternalName]): Unit = {
    for(internalName <- internalNames) {
      cache.remove(internalName)
    }
  }

  override def whereAre(internalNames: Iterable[DatasetInternalName]): Set[FoundSecondary[Secondary]] = {
    val it = internalNames.iterator
    it.nextOption() match {
      case None =>
        Set.empty
      case Some(hd) =>
        if(it.hasNext) {
          // Ok, so we're looking for a set of secondaries that _all_
          // the datasets are in.  So what we'll do is find the set of
          // secondaries for each, and then intersect-merge them.  To
          // do this, we'll build a map of secondaryName -> SR

          def mapify(secondaries: Set[SR]): Map[Secondary, SR] =
            secondaries.iterator.map { s => s.secondary -> s }.toMap

          val merged = it.map { dsin =>
            mapify(whereis1(dsin))
          }.foldLeft(mapify(whereis1(hd))) { (acc: Map[Secondary, SR], mergeSet: Map[Secondary, SR]) =>
            val commonSecondaries = acc.keySet.intersect(mergeSet.keySet)
            commonSecondaries.iterator.map { commonSecondary =>
              commonSecondary -> new SR(acc(commonSecondary).internalNames ++ mergeSet(commonSecondary).internalNames, commonSecondary)
            }.toMap
          }

          upcast(merged.valuesIterator.toSet)
        } else {
          // common case: only one dataset in the query; don't
          // bother doing the conversion to map and back
          upcast(whereis1(hd))
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
        // extract the secondaries which are "present", and possibly
        // schedule rechecking ones which are unknown or absent.
        @tailrec
        def loop(): Option[SR] = {
          v.get match {
            case present@CacheResult.Present(_) =>
              Some(new SR(Map(internalName -> (completed, present)), secondary))
            case absent@CacheResult.Absent(at, interval) if !byMe && !recheckedOne && now - at > interval =>
              val rechecking = CacheResult.Rechecking(now)
              if(v.compareAndSet(absent, rechecking)) {
                recheckedOne = true
                if(!scheduleRecheck(secondary, internalName, rechecking)) {
                  // wasn't able to schedule it
                  recheckedOne = false
                  if(v.compareAndSet(rechecking, absent)) {
                    None
                  } else {
                    // it changed out from under me.  Recheck the value that's there now
                    loop()
                  }
                } else {
                  // Scheduled a recheck, but for now we'll just say it wasn't there
                  None
                }
              } else {
                // it changed out from under me. Recheck the value that's there now
                loop()
              }
            case _ =>
              None
          }
        }
        loop()
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
    case class Job(secondary: Secondary, internalName: DatasetInternalName, expectedCacheEntry: CacheResult.Rechecking) extends Task
    case object Stop extends Task
  }

  private def scheduleRecheck(secondary: Secondary, internalName: DatasetInternalName, expectedCacheEntry: CacheResult.Rechecking): Boolean = {
    recheckerQueue.offer(Task.Job(secondary, internalName, expectedCacheEntry))
  }

  private val preallocatedUnknown = CacheResult.unknown(Instant.now())
  private class Worker extends Thread {
    setName("Rechecker worker")
    setDaemon(true)

    override def run() {
      while(!done) {
        recheckerQueue.take() match {
          case Task.Job(secondary, internalName, expectedCacheEntry) =>
            try {
              Option(cache.get(internalName)).foreach { ent =>
                ent match {
                  case completed: Cached.Completed =>
                    for(value <- completed.result.get(secondary) if value.get eq expectedCacheEntry) {
                      try {
                        try {
                          log.info("Re-checking {}:{}", secondary:Any, internalName)
                          val result = CacheResult.fromCheckResult(check(secondary, internalName))
                          value.compareAndSet(expectedCacheEntry, result)
                        } catch {
                          case e: Exception =>
                            value.compareAndSet(expectedCacheEntry, CacheResult.unknown(Instant.now()))
                            throw e
                        }
                      } catch {
                        case e: Throwable =>
                          // not ideal, but this should be the best
                          // possible chance to recover from an
                          // arbitrary error.
                          value.compareAndSet(expectedCacheEntry, preallocatedUnknown)
                          throw e
                      }
                    }
                  case _ : Cached.Pending =>
                    // nothing to do, someone else is working on it
                }
              }
            } catch {
              case e: Exception =>
                log.error("Uncaught exception in the rechecker thread", e)
            }
          case Task.Stop =>
            done = true
            recheckerQueue.offer(Task.Stop)
        }
      }
    }
  }
}

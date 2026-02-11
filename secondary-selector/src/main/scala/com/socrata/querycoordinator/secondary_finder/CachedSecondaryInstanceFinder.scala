package com.socrata.querycoordinator.secondary_finder

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

import java.util.concurrent.{ConcurrentHashMap, ArrayBlockingQueue, ThreadLocalRandom}
import java.util.concurrent.atomic.{AtomicReference, AtomicLong}

import com.rojoma.simplearm.v2.Resource
import com.rojoma.json.v3.ast.{JObject, JNumber, JString}
import com.rojoma.json.v3.codec.{JsonEncode, FieldEncode}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.util.{AutomaticJsonEncodeBuilder, SimpleHierarchyEncodeBuilder, InternalTag}
import com.typesafe.scalalogging.Logger

import com.socrata.querycoordinator.util.{IteratorExt, DoubleExt, MonotoneInstant}

object CachedSecondaryInstanceFinder {
  private val log = Logger[CachedSecondaryInstanceFinder[_, _]]

  sealed abstract class CheckResult
  object CheckResult {
    case object Present extends CheckResult
    case object Absent extends CheckResult
    case object Unknown extends CheckResult
  }

  implicit def resource[DIN, S] = new Resource[CachedSecondaryInstanceFinder[DIN, S]] {
    override def close(csif: CachedSecondaryInstanceFinder[DIN, S]): Unit =
      csif.stop()
  }
}

// The way this cache works is:
//    "Yes, I have it" results are cached until explicitly invalidated.
//    At most once every absentInterval, "Absent" results may be re-checked.
//    At most once every unknownInterval, "Unknown" results may be re-checked.
// When a req comes in for a dataset with at least one
// sufficiently-old absent or unknown response, one of them it will be
// chosen to be re-checked.  When this happens the timestamp on it is
// reset.
class CachedSecondaryInstanceFinder[DatasetInternalName, Secondary](
  allSecondariesRaw: Set[Secondary],
  check: (Secondary, DatasetInternalName) => CachedSecondaryInstanceFinder.CheckResult,
  absentInterval: FiniteDuration,
  absentBound: FiniteDuration,
  unknownInterval: FiniteDuration,
  unknownBound: FiniteDuration
) extends SecondaryInstanceFinder[DatasetInternalName, Secondary] {
  import CachedSecondaryInstanceFinder._

  private val allSecondariesSeq = allSecondariesRaw.toSeq
  override val allSecondaries = allSecondariesRaw.map(new FoundSecondary.Simple(_))

  private val recheckers: Seq[Worker] = (1 to 20).map(new Worker(_))
  @volatile private var done = false;
  private val recheckerQueue = new ArrayBlockingQueue[Task](100)

  private def singleton[T] = new JsonEncode[T] {
    def encode(t: T) = JObject.canonicalEmpty
  }

  private implicit object finiteDurationEncode extends JsonEncode[FiniteDuration] {
    override def encode(d: FiniteDuration) = JNumber(d.toNanos)
  }

  private sealed abstract class NotPresent {
    def initialInterval: FiniteDuration
    def bound: FiniteDuration
  }
  private object NotPresent {
    case object Absent extends NotPresent {
      override def initialInterval = absentInterval
      override def bound = absentBound
    }
    case object Unknown extends NotPresent {
      override def initialInterval = unknownInterval
      override def bound = unknownBound
    }

    implicit val jEncode = new JsonEncode[NotPresent] {
      private val absent = JString("absent")
      private val unknown = JString("unknown")
      override def encode(np: NotPresent) =
        np match {
          case Absent => absent
          case Unknown => unknown
        }
    }
  }

  def start() {
    recheckers.foreach(_.start())
  }

  def stop() {
    done = true
    recheckerQueue.offer(Task.Stop) // doesn't matter if this successfully enqueues; if it doesn't, there's pending work for the checker to see
    recheckers.foreach(_.join())
  }

  private sealed abstract class CacheResult {
    val checkedAt: MonotoneInstant
  }
  private object CacheResult {
    case class Present(checkedAt: MonotoneInstant) extends CacheResult
    case class Absent(kind: NotPresent, checkedAt: MonotoneInstant, checkAfter: FiniteDuration) extends CacheResult
    case class Rechecking(kind: NotPresent, checkedAt: MonotoneInstant, nextCheckPause: FiniteDuration) extends CacheResult

    def fromCheckResult(checkResult: CheckResult): CacheResult = {
      checkResult match {
        case CheckResult.Present => Present(MonotoneInstant.now())
        case CheckResult.Absent => absent(MonotoneInstant.now())
        case CheckResult.Unknown => unknown(MonotoneInstant.now())
      }
    }

    def absent(checkedAt: MonotoneInstant) = Absent(NotPresent.Absent, checkedAt, absentInterval)
    def unknown(checkedAt: MonotoneInstant) = Absent(NotPresent.Unknown, checkedAt, unknownInterval)

    implicit val jEncode = SimpleHierarchyEncodeBuilder[CacheResult](InternalTag("type"))
      .branch[Present]("present")(AutomaticJsonEncodeBuilder[Present], implicitly)
      .branch[Absent]("absent")(AutomaticJsonEncodeBuilder[Absent], implicitly)
      .branch[Rechecking]("rechecking")(AutomaticJsonEncodeBuilder[Rechecking], implicitly)
      .build
  }

  private sealed abstract class Cached
  private object Cached {
    // these are deliberately not case classes.  They have identity
    // semantics, and Pending requires holding a lock to access the
    // result field.
    class Completed(val result: Map[Secondary, AtomicReference[CacheResult]], val count: AtomicLong) extends Cached
    object Completed {
      implicit def jEncode(implicit ev: FieldEncode[Secondary]) = new JsonEncode[Completed] {
        override def encode(v: Completed) =
          json"""{
            result: ${v.result.mapValues(_.get)},
            count: ${v.count.get}
          }"""
      }
    }
    class Pending(var result: Option[Completed]) extends Cached

    implicit def jEncode(implicit ev: FieldEncode[Secondary]) =
      SimpleHierarchyEncodeBuilder[Cached](InternalTag("type"))
        // this will do a non-synchronized access to Pending#result,
        // but since it's just for debug output that should be fine...
        .branch[Pending]("pending")(AutomaticJsonEncodeBuilder[Pending], implicitly)
        .branch[Completed]("completed")
        .build
  }
  private val cache = new ConcurrentHashMap[DatasetInternalName, Cached]

  def toJValue(implicit din: FieldEncode[DatasetInternalName], s: FieldEncode[Secondary]) =
    JsonEncode.toJValue(cache.asScala)

  private class SR(
    val internalNames: Map[DatasetInternalName, (Cached.Completed, CacheResult.Present)],
    override val secondary: Secondary
  ) extends FoundSecondary[Secondary] {
    override def invalidateAll(): Unit = {
      for((internalName, (origCompleted, _)) <- internalNames) {
        log.debug("Invalidating all secondaries associated with {}", internalName)
        cache.remove(internalName, origCompleted)
      }
    }

    override def invalidateSecondary(): Unit = {
      val now = MonotoneInstant.now()
      for((internalName, (_, origPresent)) <- internalNames) {
        Option(cache.get(internalName)).foreach {
          case completed: Cached.Completed =>
            completed.result.get(secondary).foreach { v =>
              log.debug("Marking {}/{} as unknown", secondary, internalName)
              v.compareAndSet(origPresent, CacheResult.unknown(now))
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

  override def softInvalidate(internalNames: Iterable[DatasetInternalName]): Unit = {
    for(internalName <- internalNames) {
      def softReset(c: Cached.Completed): Unit = {
        for(v <- c.result.valuesIterator) {
          v.get match {
            case CacheResult.Present(_) => // ok
            case absent: CacheResult.Absent => v.compareAndSet(absent, CacheResult.unknown(absent.checkedAt))
            case rechecking: CacheResult.Rechecking => v.compareAndSet(rechecking, rechecking.copy(kind = NotPresent.Unknown, nextCheckPause = unknownInterval))
          }
        }
      }

      @tailrec def loop(): Unit = {
        Option(cache.get(internalName)) match {
          case Some(c: Cached.Completed) =>
            softReset(c)
          case Some(p: Cached.Pending) =>
            p.synchronized { p.result } match {
              case Some(c) => softReset(c)
              case None => loop()
            }
          case None =>
            // ok
        }
      }
      loop()
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

  private def nextInterval(kind: NotPresent, lastInterval: FiniteDuration): FiniteDuration = {
    val bound = kind.bound

    val candidate = lastInterval * 2
    val unjittered = if(candidate < bound) {
      candidate
    } else {
      bound
    }

    unjittered // + (2.5 * ThreadLocalRandom.current().nextGaussian()).clamp(-5.0, 5.0).seconds
  }

  private def whereis1(internalName: DatasetInternalName): Set[SR] = {
    val WhereisResult(completed, byMe) = doWhereis(internalName)
    completed.count.getAndIncrement()

    var recheckedOne = false
    val now = MonotoneInstant.now()
    val rng = new Random(ThreadLocalRandom.current())
    val result = rng.shuffle(completed.result.toSeq)
      .iterator
      .flatMap { case (secondary, v) =>
        // extract the secondaries which are "present", and possibly
        // schedule rechecking ones which are unknown or absent.
        // We'll recheck at most one per pass, and count on the
        // shuffling to spread that out across the candidate
        // secondaries.
        @tailrec
        def loop(): Option[SR] = {
          v.get match {
            case present@CacheResult.Present(_) =>
              log.debug("Found {} in {} (cached {})", internalName, secondary, present.checkedAt)
              Some(new SR(Map(internalName -> (completed, present)), secondary))
            case absent@CacheResult.Absent(kind, at, interval) if !byMe && !recheckedOne && now - at > interval =>
              val rechecking = CacheResult.Rechecking(kind, now, nextInterval(kind, interval))
              if(v.compareAndSet(absent, rechecking)) {
                recheckedOne = true
                log.debug("Scheduling recheck for {}/{} ({} elapsed; {} interval)", secondary, internalName, now - at, interval)
                if(!scheduleRecheck(secondary, internalName)) {
                  // wasn't able to schedule it
                  log.debug("Failed to schedule recheck for {}/{}", secondary, internalName)
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
            case absent@CacheResult.Absent(kind, at, interval) if !byMe && !recheckedOne && now - at <= interval =>
              log.debug("Not scheduling recheck because {} <= {}", now - at, interval)
              None
            case _ : CacheResult.Rechecking =>
              log.debug("Not scheduling recheck because already in the rechecking state")
              None
            case _ =>
              None
          }
        }
        loop()
      }
      .toSet

    // if I just did the lookup myself, or if there are "unknown"
    // answers that were not previously absent, accept returning an
    // empty result
    if(result.nonEmpty || byMe) {
      result
    } else {
      // there were none available - panic!  Well, not really.
      // Instead, we'll recheck anything that has been sitting around
      // longer than the _minimal_ recheck threshold.
      log.debug("Had a cached result where there were no secondaries for {}", internalName)

      val newCompleted = doReWhereis(internalName, completed)

      // If we _still_ don't have any, well.. guess we're returning an
      // empty set at this point.
      newCompleted.result
        .iterator
        .flatMap { case (secondary, v) =>
          v.get match {
            case present@CacheResult.Present(_) =>
              log.debug("Found {} in {} (cached {})", internalName, secondary, present.checkedAt)
              Some(new SR(Map(internalName -> (completed, present)), secondary))
            case _ =>
              None
          }
        }
        .toSet
    }
  }

  private def doCheck(secondary: Secondary, name: DatasetInternalName): CacheResult = {
    val result = try {
      check(secondary, name)
    } catch {
      case e: Exception =>
        log.warn("Unexpected exception while checking {} in {}", name, secondary, e)
        CheckResult.Unknown
    }
    log.debug("Checked {}/{}: {}", name, secondary, result)
    CacheResult.fromCheckResult(result)
  }

  private case class WhereisResult(completed: Cached.Completed, byMe: Boolean)
  private def doWhereis(internalName: DatasetInternalName): WhereisResult = {
    val pending = new Cached.Pending(None)
    pending.synchronized {
      @tailrec
      def loop(): WhereisResult = {
        log.debug("Looking up {}", internalName)
        cache.computeIfAbsent(internalName, (_) => pending) match {
          case p: Cached.Pending if p eq pending =>
            // I am doing this check
            try {
              log.debug("Seeking {} in all secondaries", internalName)

              val checkResults: Map[Secondary, AtomicReference[CacheResult]] = allSecondariesSeq.par
                .map { secondary =>
                  val result = doCheck(secondary, internalName)
                  (secondary, new AtomicReference(result))
                }
                .seq
                .toMap

              val result = new Cached.Completed(checkResults, new AtomicLong(0))
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
            log.debug("Waiting for some other thread to resolve {}", internalName)
            otherPending.synchronized { otherPending.result } match {
              case Some(result) =>
                if(cache.replace(internalName, otherPending, result)) {
                  log.warn("Unexpected success: replacing completed pending with its associated result")
                }
                WhereisResult(result, byMe = false)
              case None =>
                if(cache.remove(internalName, otherPending)) {
                  log.warn("Unexpected success: removing uncompleted pending")
                }
                loop() // they didn't finish normally, so try again ourselves
            }
          case result: Cached.Completed =>
            // there was already a cached value, return it
            log.debug("Found a cached value for {}", internalName)
            WhereisResult(result, byMe = false)
        }
      }
      loop()
    }
  }

  // The value is already cached, but we we ant to poke any results
  // which have been in the cache for the minimal amount of time, so
  // we'll do _basically_ the same thing as doWhereis.  In the end, we
  // won't actually be replacing the cache entries, just the values
  // contained in its result.
  private def doReWhereis(internalName: DatasetInternalName, orig: Cached.Completed): Cached.Completed = {
    val pending = new Cached.Pending(Some(orig))

    pending.synchronized {
      log.debug("Re-looking up {}", internalName)
      if(cache.replace(internalName, orig, pending)) {
        try {
          for((secondary, cacheResult) <- orig.result.par) {
            cacheResult.get match {
              case CacheResult.Absent(kind, checkedAt, _) if checkedAt.elapsed > kind.initialInterval =>
                log.debug("Re-looking up {} in {}", internalName, secondary)
                val result = doCheck(secondary, internalName)
                cacheResult.set(result)
              case _ =>
                // ok
            }
          }

          orig
        } finally {
          // Whether or not we're exiting abnormally, remove our
          // pending claim.
          cache.put(internalName, orig)
        }
      } else {
        // the cached value was already replaced by someone else, so
        // just do "doWhereis".  We don't need to drop our lock on
        // pending as no other thread will ever see it.
        log.debug("..actually just doing a normal whereis as cache.replace failed")
        doWhereis(internalName).completed
      }
    }
  }

  private sealed abstract class Task
  private object Task {
    case class Job(secondary: Secondary, internalName: DatasetInternalName) extends Task
    case object Stop extends Task
  }

  private def scheduleRecheck(secondary: Secondary, internalName: DatasetInternalName): Boolean = {
    recheckerQueue.offer(Task.Job(secondary, internalName))
  }

  private val preallocatedUnknown = CacheResult.unknown(MonotoneInstant.now())
  private class Worker(n: Int) extends Thread {
    setName(s"Rechecker worker $n")
    setDaemon(true)

    override def run() {
      while(!done) {
        recheckerQueue.take() match {
          case job@Task.Job(secondary, internalName) =>
            try {
              log.debug("Processing recheck job {}", job)

              def processCompleted(completed: Cached.Completed): Unit = {
                val secondaryEntry = completed.result.get(secondary)
                log.debug("Current secondary state: {}", secondaryEntry)
                secondaryEntry.foreach { value =>
                  value.get match {
                    case orig@CacheResult.Rechecking(kind, checkedAt, nextCheckPause) =>
                      try {
                        log.debug("Re-checking {}/{} ({}ms after enqueue)", secondary, internalName, checkedAt.elapsed.toMillis)
                        val result = doCheck(secondary, internalName) match {
                          case absent: CacheResult.Absent if absent.kind == kind =>
                            absent.copy(checkAfter = nextCheckPause)
                          case other =>
                            other
                        }
                        log.debug("Checked {}/{}: {}", internalName, secondary, result)
                        value.set(result)
                      } catch {
                        case e: Throwable =>
                          // not ideal, but this should be the best
                          // possible chance to recover from an arbitrary
                          // error.
                          value.compareAndSet(orig, preallocatedUnknown)
                          throw e
                      }
                    case other =>
                      log.debug("Not in a 'rechecking' state: {}", other)
                  }
                }
              }

              @tailrec def loop(): Unit = {
                Option(cache.get(internalName)) match {
                  case Some(completed: Cached.Completed) =>
                    processCompleted(completed)
                  case Some(pending : Cached.Pending) =>
                    log.debug("Someone else is working on this; waiting for them")
                    pending.synchronized { pending.result } match {
                      case Some(completed) => processCompleted(completed)
                      case None => loop()
                    }
                  case None =>
                    log.debug("No cache entry found for {}??", internalName)
                }
              }
              loop()
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

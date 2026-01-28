package com.socrata.querycoordinator.secondary_selector

abstract class FoundSecondary[Secondary] {
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
      case s: FoundSecondary[_] => this.secondary == s.secondary
      case _ => false
    }

  override final def toString =
    s"FoundSecondary($secondary)"
}

object FoundSecondary {
  // A pure secondary, without any "dataset" involved
  class Simple[Secondary](val secondary: Secondary) extends FoundSecondary[Secondary] {
    override def invalidateAll() = {}
    override def invalidateSecondary() = {}
  }
}

trait SecondaryInstanceFinder[DatasetInternalName, Secondary] {
  val allSecondaries: Set[FoundSecondary[Secondary]]

  def invalidate(internalNames: Iterable[DatasetInternalName]): Unit

  // Find a secondary containing _all_ the given datasets
  def whereAre(internalNames: Iterable[DatasetInternalName]): Set[FoundSecondary[Secondary]]

  final def whereIs(internalName: DatasetInternalName): Set[FoundSecondary[Secondary]] =
    whereAre(internalName :: Nil)
}



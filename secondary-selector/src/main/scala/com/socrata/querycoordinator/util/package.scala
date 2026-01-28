package com.socrata.querycoordinator

package object util {
  implicit class IteratorExt[T](private val it: Iterator[T]) extends AnyVal {
    def nextOption(): Option[T] = {
      if(it.hasNext) {
        Some(it.next())
      } else {
        None
      }
    }
  }
}

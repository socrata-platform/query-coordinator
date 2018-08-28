package com.socrata.querycoordinator.exceptions

class JoinedDatasetNotColocatedException(val dataset: String, val secondaryHost: String) extends
  Exception(s"joined dataset $dataset not colocated in $secondaryHost")

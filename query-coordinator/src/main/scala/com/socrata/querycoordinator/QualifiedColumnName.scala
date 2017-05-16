package com.socrata.querycoordinator

import com.socrata.soql.environment.ColumnName


case class QualifiedColumnName(val qualifier: Option[String], val columnName: ColumnName)


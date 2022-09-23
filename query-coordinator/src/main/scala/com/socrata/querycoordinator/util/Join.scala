package com.socrata.querycoordinator.util

import com.socrata.soql.environment.{ColumnName, DatasetContext, TableName, UntypedDatasetContext}
import com.socrata.soql.typed.Qualifier
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.soql.{AnalysisContext, ParameterSpec}

object Join {

  val NoQualifier: Qualifier = None

  // TODO: Join - from one to multiple contexts
  def toAnalysisContext(ctx: DatasetContext[SoQLType], parameters: ParameterSpec[SoQLType, SoQLValue] = ParameterSpec.empty): AnalysisContext[SoQLType, SoQLValue] = {
    AnalysisContext(
      schemas = Map(TableName.PrimaryTable.qualifier -> ctx),
      parameters = parameters
    )
  }

  def toUntypedAnalysisContext(ctx: UntypedDatasetContext): Map[String, UntypedDatasetContext] = {
    Map(TableName.PrimaryTable.qualifier -> ctx)
  }

  // TODO: Join - qualifier
  def mapIgnoringQualifier[NewColumnId](map: ColumnName => NewColumnId)(columnName: ColumnName, qualifier: Qualifier)
    : NewColumnId = {
    map(columnName)
  }
}

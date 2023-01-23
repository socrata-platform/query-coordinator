package com.socrata.querycoordinator.rollups

import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.json.v3.codec.JsonDecode
import com.socrata.querycoordinator._
import com.socrata.soql._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, DatasetContext}
import com.socrata.soql.types.{SoQLType, SoQLValue}

import scala.io.Source

abstract class BaseConfigurableRollupTest extends TestBase {

  type SchemaConfig = Map[String, (SoQLType, String)]
  def getConfig[T:JsonDecode](configFile: String): T = {
    val resource = Source.fromResource(configFile)
    JsonUtil.readJson[T](resource.reader()).right.get
  }

  def getContext(schemas: Map[String, SchemaConfig]): AnalysisContext[SoQLType, SoQLValue] = {
    AnalysisContext[SoQLType, SoQLValue](
      schemas = schemas.mapValues { schemaWithFieldName =>
        val columnNameToType = schemaWithFieldName.map {
          case (_iColumnId, (typ, fieldName)) =>
            (ColumnName(fieldName), typ)
        }
        new DatasetContext[SoQLType] {
          val schema: OrderedMap[ColumnName, SoQLType] =
            OrderedMap[ColumnName, SoQLType](columnNameToType.toSeq: _*)
        }
      },
      parameters = ParameterSpec.empty
    )
  }

  def getColumnMapping(schemas: Map[String, Map[String, (SoQLType, String)]]): Map[QualifiedColumnName, String] = {
    schemas.foldLeft(Map.empty[QualifiedColumnName, String]) { (acc, entry) =>
      val (tableName, schemaWithFieldName) = entry
      val map = schemaWithFieldName.map {
        case (iColumnId, (_typ, fieldName)) =>
          val qual = if (tableName == "_") None else Some(tableName)
          QualifiedColumnName(qual, ColumnName(fieldName)) -> iColumnId
      }
      acc ++ map
    }
  }

}

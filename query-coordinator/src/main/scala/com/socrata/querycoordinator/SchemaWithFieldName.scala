package com.socrata.querycoordinator

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.matcher.{PObject, Variable}
import com.socrata.querycoordinator.util.SoQLTypeCodec
import com.socrata.soql.types.SoQLType

case class SchemaWithFieldName(hash: String, schema: Map[String, (SoQLType, String)], pk: String) {

  def toSchema(): Schema = {
    val schemaLessFieldName = schema.map {
      case ( columnId, (typ, _)) => (columnId -> typ)
    }
    Schema(hash, schemaLessFieldName, pk)
  }
}


object SchemaWithFieldName {
  implicit object SchemaWithFieldNameCodec extends JsonDecode[SchemaWithFieldName] with JsonEncode[SchemaWithFieldName] {
    private implicit val soQLTypeCodec = SoQLTypeCodec

    private val hashVar = Variable[String]()
    private val schemaVar = Variable[Map[String, (SoQLType, String)]]()
    private val pkVar = Variable[String]()
    private val PSchema = PObject(
      "hash" -> hashVar,
      "schema" -> schemaVar,
      "pk" -> pkVar
    )

    def encode(schemaObj: SchemaWithFieldName): JValue = {
      val SchemaWithFieldName(hash, schema, pk) = schemaObj
      PSchema.generate(hashVar := hash, schemaVar := schema, pkVar := pk)
    }

    def decode(x: JValue): Either[DecodeError, SchemaWithFieldName] = PSchema.matches(x) match {
      case Right(results) => Right(SchemaWithFieldName(hashVar(results), schemaVar(results), pkVar(results)))
      case Left(ex) => Left(ex)
    }
  }
}

package com.socrata.querycoordinator

import com.socrata.http.client.RequestBuilder

object FakeSchemaFetcher extends SchemaFetcher {

  import SchemaFetcher._

  def apply(base: RequestBuilder, dataset: String, copy: Option[String], useResourceName: Boolean = false): Result = {
    NonSchemaResponse
  }

  def schemaWithFieldName(base: RequestBuilder, dataset: String, copy: Option[String], useResourceName: Boolean = false): Result = {
    NonSchemaResponse
  }
}

import SchemaFetcher._

class FakeSchemaFetcher(result: Result) extends SchemaFetcher {

  def apply(base: RequestBuilder, dataset: String, copy: Option[String], useResourceName: Boolean = false): Result = {
    NonSchemaResponse
  }

  def schemaWithFieldName(base: RequestBuilder, dataset: String, copy: Option[String], useResourceName: Boolean = false): Result = {
    result
  }
}

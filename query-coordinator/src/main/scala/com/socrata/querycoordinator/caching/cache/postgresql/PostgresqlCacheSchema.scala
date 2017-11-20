package com.socrata.querycoordinator.caching.cache.postgresql

import java.sql.{Connection, SQLException}
import javax.sql.DataSource

import com.mchange.v2.resourcepool.TimeoutException
import com.rojoma.simplearm.v2._

import scala.concurrent.duration.FiniteDuration

object PostgresqlCacheSchema {
  private def requireConnection(dataSource: DataSource): Connection = {
    var c: Connection = null
    do {
      try {
        c = dataSource.getConnection()
      } catch {
        case e: SQLException if e.getCause.isInstanceOf[TimeoutException] =>
          // ok, we'll just retry until we get one
      }
    } while(c == null)
    c
  }

  val table: String = "cache"

  def dataTable(dataset: String): String = s"cache_${dataset}_data"

  val pendingDeleteTable: String = "pending_deletes"

  def create(dataSource: DataSource, dataset: Option[String]): Unit = {
    using(requireConnection(dataSource)) { conn =>
      conn.setAutoCommit(false)

      try {
        val md = conn.getMetaData

        def exists(table: String): Boolean =
          using(md.getTables(conn.getCatalog, "public", table, Array("TABLE"))) { rs =>
            rs.next()
          }

        using(conn.createStatement()) { stmt =>
          // ok, so "cache" is sort of interesting.  There's a unique constraint on "key" but it is nullable; there can
          // be more than one null value in a column with a unique constraint, and we're taking advantage of that.  A
          // row with a null key is either being created or pending deletion; the decision is based on a combination of
          // being referred to from the pending_deletes table (in which case it's _definitely_ pending delete) or having
          // an old "created_at" timestamp (in which case likely something happened to prevent the population of the data
          // from being completed).
          //
          // pending_deletes exists so that we can mark an entry for deletion and be (reasonably) assured that anything
          // currently using that entry will finish successfully.  The actual delete happens asynchronously.
          if (dataset.isEmpty) {
            if (!exists(table)) {
              stmt.execute(s"create table ${table} (id bigserial not null primary key, dataset text not null, key text unique, created_at timestamp with time zone not null, approx_last_access timestamp with time zone not null)")
              stmt.execute(s"create index ${table}_created_at_idx on ${table}(created_at) where key is null")
              stmt.execute(s"create index ${table}_access_idx on ${table}(approx_last_access)")
              stmt.execute(s"create index ${table}_dataset_access_idx on ${table}(dataset, approx_last_access)")
            }

            if (!exists(pendingDeleteTable)) {
              stmt.execute(s"create table ${pendingDeleteTable} (id bigserial not null primary key, dataset text not null, cache_id bigint not null, delete_queued timestamp with time zone not null)")
              stmt.execute(s"create index ${pendingDeleteTable}_delete_queued_idx on ${pendingDeleteTable}(delete_queued)")
              stmt.execute(s"create index ${pendingDeleteTable}_dataset_delete_queued_idx on ${pendingDeleteTable}(dataset, delete_queued)")
            }

            if (!exists("clean_run")) {
              stmt.execute("create table clean_run(id text not null, active boolean unique, updated_at timestamp with time zone not null)")
            }
          }

          for (ds <- dataset) {
            val dataTableName = dataTable(ds)
            if (!exists(dataTableName)) {
              stmt.execute(s"create table ${dataTableName} (cache_id bigint not null, chunknum int not null, data bytea not null, primary key(cache_id, chunknum))")
            }
          }
        }

        conn.commit()
      } finally {
        conn.rollback()
      }
    }
  }

  def setTimeout(conn: Connection, timeout: FiniteDuration): Unit = {
    using (conn.createStatement()) { stmt =>
      stmt.execute(s"SET statement_timeout TO ${timeout.toMillis}")
    }
  }
}

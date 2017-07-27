package com.socrata.querycoordinator.caching.cache.postgresql

import java.sql._
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

import com.socrata.querycoordinator.caching.cache.CacheCleaner

import scala.concurrent.duration.FiniteDuration
import com.rojoma.simplearm.v2._
import org.joda.time.DateTime
import org.postgresql.util.PSQLException

/**
 *
 * @param dataSource
 * @param survivorCutoff TTL for a normal cache entry
 * @param deleteDelay Amount of time for an entry to remain on the pending_delete queue before it is actually deleted
 * @param assumeDeadCreateCutoff Amount of time for a keyless entry not on the delete queue to live before it is moved to the delete queue
 * @param chunkSize Number of rows to delete from the cache tables in a single roundtrip
 * @param useBatch True to us a batch of single-row deletes for each chunk; false to do DELETE IN (...).  It looks like
 *                   DELETE IN (...) may suppress index-use?
 */
class PostgresqlCacheCleaner(dataSource: DataSource, survivorCutoff: FiniteDuration, deleteDelay: FiniteDuration, assumeDeadCreateCutoff: FiniteDuration, chunkSize: Int, useBatch: () => Boolean, interval: FiniteDuration, id: String) extends CacheCleaner {

  import PostgresqlCacheSchema.setTimeout

  val timeout = FiniteDuration(2, TimeUnit.HOURS)

  val survivorCutoffMS = survivorCutoff.toMillis
  val deleteDelayMS = deleteDelay.toMillis
  val assumeDeadCreateCutoffMS = assumeDeadCreateCutoff.toMillis

  val log = org.slf4j.LoggerFactory.getLogger(classOf[PostgresqlCacheCleaner])

  def multiSql(conn: Connection, batchSql: String, chunkSql: Iterable[Long] => String, ids: Iterable[Long]): Int = {
    if(ids.nonEmpty) {
      if(useBatch()) {
        using(conn.prepareStatement(batchSql)) { stmt =>
          ids.grouped(chunkSize).foldLeft(0) { (updatedSoFar, chunk) =>
            for(id <- chunk) {
              stmt.setLong(1, id)
              stmt.addBatch()
            }
            updatedSoFar + stmt.executeBatch().sum
          }
        }
      } else {
        using(conn.createStatement()) { stmt =>
          ids.grouped(chunkSize).foldLeft(0) { (updatedSoFar, chunk) =>
            updatedSoFar + stmt.executeUpdate(chunkSql(chunk))
          }
        }
      }
    } else {
      0
    }
  }

  def multiDelete(conn: Connection, table: String, idCol: String, ids: Iterable[Long]): Int = {
    multiSql(conn, s"DELETE FROM $table WHERE $idCol = ?", _.mkString(s"DELETE FROM $table WHERE $idCol IN (", ",", ")"), ids)
  }

  def multiUpdate(conn: Connection, table: String, update: String, idCol: String, ids: Iterable[Long]): Int = {
    multiSql(conn, s"UPDATE $table SET $update WHERE $idCol = ?", _.mkString(s"UPDATE $table SET $update WHERE $idCol IN (", ",", ")"), ids)
  }

  def clean(): Unit = {
    using(dataSource.getConnection()) { conn =>
      conn.setAutoCommit(true)
      try {
        using(conn.createStatement()) { stmt =>
          using(stmt.executeQuery("select max(updated_at) from clean_run where active is null")) { rs =>
            rs.next()
            Option(rs.getTimestamp(1)).foreach { lastClean =>
              if (System.currentTimeMillis() < lastClean.getTime + interval.toMillis) {
                return
              }
            }
          }
        }
        using(conn.prepareStatement("insert into clean_run values(?, ?, ?)")) { stmt =>
          stmt.setString(1, id)
          stmt.setBoolean(2, true)
          stmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()))
          stmt.executeUpdate()
        }
        serializedClean(conn)
      } catch {
        case ex: PSQLException if "23505" == ex.getSQLState =>
          conn.clearWarnings()
          return
      } finally {
        conn.setAutoCommit(true)
        using(conn.createStatement()) { stmt =>
          stmt.executeUpdate("delete from clean_run where updated_at < now() - interval '5 day'")
        }
        using(conn.prepareStatement("update clean_run set active = null, updated_at = now() where id = ? and active")) { stmt =>
          stmt.setString(1, id)
          stmt.executeUpdate()
        }
      }
    }
  }

  def serializedClean(conn: Connection): Unit = {

    conn.setAutoCommit(false)
    conn.setReadOnly(false)

    def stage1(): Boolean = {
      log.info("cache clean stage 1 starts")
      val oldIsolation = conn.getTransactionIsolation
      try {
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

        val hardDeleteCutoff = new Timestamp(System.currentTimeMillis - deleteDelayMS)

        using(conn.prepareStatement("SELECT dataset FROM pending_deletes WHERE delete_queued < ? GROUP BY dataset")) { datasetStmt =>
          setTimeout(conn, timeout)
          datasetStmt.setTimestamp(1, hardDeleteCutoff)
          using(datasetStmt.executeQuery()) { rs =>
            while (rs.next()) {
              val dataset = rs.getString(1)
              setTimeout(conn, timeout)
              using(conn.prepareStatement(s"DELETE FROM cache_${dataset}_data WHERE cache_id in (SELECT cache_id FROM pending_deletes WHERE delete_queued < ? and dataset = ?)")) { deleteStmt =>
                deleteStmt.setTimestamp(1, hardDeleteCutoff)
                deleteStmt.setString(2, dataset)
                deleteStmt.executeUpdate()
                conn.commit()
              }

              using(conn.createStatement()) { stmt =>
                // delete empty cache_data table
                try {
                  stmt.executeUpdate(s"LOCK table cache_${dataset}_data NOWAIT")
                  using(stmt.executeQuery(s"SELECT cache_id FROM cache_${dataset}_data limit 1")) { rs =>
                    if (!rs.next()) {
                        stmt.executeUpdate(s"DROP TABLE cache_${dataset}_data")
                    }
                  }
                  conn.commit()
                  log.info(s"drop empty table cache_${dataset}_data")
                } catch {
                  case ex: PSQLException =>
                    log.warn(s"cannot drop cache_${dataset}_data", ex)
                    conn.rollback()
                }
              }

              setTimeout(conn, timeout)
              using(conn.prepareStatement(s"DELETE FROM cache WHERE id in (SELECT cache_id FROM pending_deletes WHERE delete_queued < ? and dataset = ?)")) { deleteStmt =>
                deleteStmt.setTimestamp(1, hardDeleteCutoff)
                deleteStmt.setString(2, dataset)
                deleteStmt.executeUpdate()
                conn.commit()
              }
              setTimeout(conn, timeout)
              using(conn.prepareStatement(s"DELETE FROM pending_deletes WHERE cache_id in (SELECT cache_id FROM pending_deletes WHERE delete_queued < ? and dataset = ?)")) { deleteStmt =>
                deleteStmt.setTimestamp(1, hardDeleteCutoff)
                deleteStmt.setString(2, dataset)
                val deleted = deleteStmt.executeUpdate()
                conn.commit()
              }
            }
          }
        }

        // ok, now we need to kill any cache entries with a NULL key and an old enough created_at and which are NOT in pending_deletes
        using(conn.prepareStatement("SELECT dataset FROM cache WHERE key IS NULL and created_at < ? AND id NOT IN (SELECT cache_id FROM pending_deletes) GROUP BY dataset")) { stmt =>
          val ts = new Timestamp(System.currentTimeMillis - assumeDeadCreateCutoffMS)
          stmt.setTimestamp(1, ts)
          setTimeout(conn, timeout)
          using(stmt.executeQuery()) { rs =>
            while (rs.next()) {
              val dataset = rs.getString(1)
              setTimeout(conn, timeout)
              using(conn.prepareStatement(s"DELETE FROM cache_${dataset}_data WHERE cache_id in (SELECT id FROM cache WHERE key IS NULL and created_at < ?) AND id NOT IN (SELECT cache_id FROM pending_deletes)")) { deleteStmt =>
                deleteStmt.setTimestamp(1, ts)
                deleteStmt.executeUpdate()
                conn.commit()
              }
              if (rs.isLast) {
                setTimeout(conn, timeout)
                using(conn.prepareStatement("DELETE FROM cache WHERE in (SELECT id FROM cache WHERE key IS NULL and created_at < ?) AND id NOT IN (SELECT cache_id FROM pending_deletes)")) { deleteStmt =>
                  deleteStmt.setTimestamp(1, ts)
                  deleteStmt.executeUpdate()
                  conn.commit()
                }
              }
            }
          }
        }
        log.info("cache clean stage 1 ends")
        true
      } catch {
        case e: SQLException if e.getSQLState == "40001" /* serialization_failure */ =>
          false
      } finally {
        conn.rollback()
        conn.setTransactionIsolation(oldIsolation)
      }
    }

    while(!stage1()) {}

    // ok, we've deleted everything that's definitively dead
    // so now let's kill things that have expired

    def stage2(): Boolean = {
      log.info("cache clean stage 2 starts")
      val oldIsolation = conn.getTransactionIsolation
      try {
        conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
        setTimeout(conn, timeout)
        val deletedCacheEntries =
          using(conn.prepareStatement("INSERT INTO pending_deletes (cache_id, dataset, delete_queued) SELECT id, dataset, now() FROM cache WHERE key IS NOT NULL AND approx_last_access < ? RETURNING cache_id")) { stmt =>
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis - survivorCutoffMS))
            using(stmt.executeQuery()) { rs =>
              val res = Set.newBuilder[Long]
              while(rs.next()) {
                res += rs.getLong(1)
              }
              res.result()
            }
          }
        multiUpdate(conn, "cache", "key = NULL", "id", deletedCacheEntries)
        log.info("cache clean stage 2 ends.  Added {} entries to the pending delete queue", deletedCacheEntries.size)
        conn.commit()
        true
      } catch {
        case e: SQLException if e.getSQLState == "40001" /* serialization_failure */ =>
          false
      } finally {
        conn.rollback()
        conn.setTransactionIsolation(oldIsolation)
      }
    }

    while(!stage2()) {}

  }
}

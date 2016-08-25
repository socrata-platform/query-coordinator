package com.socrata.querycoordinator.caching.cache.postgresql

import java.sql.{Statement, SQLException, Connection, Timestamp}
import javax.sql.DataSource

import com.socrata.querycoordinator.caching.cache.CacheCleaner

import scala.concurrent.duration.FiniteDuration
import com.rojoma.simplearm.v2._

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
class PostgresqlCacheCleaner(dataSource: DataSource, survivorCutoff: FiniteDuration, deleteDelay: FiniteDuration, assumeDeadCreateCutoff: FiniteDuration, chunkSize: Int, useBatch: () => Boolean) extends CacheCleaner {
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
      conn.setAutoCommit(false)
      conn.setReadOnly(false)

      def stage1(): Boolean = {
        val oldIsolation = conn.getTransactionIsolation
        try {
          conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

          val hardDeleteCutoff = new Timestamp(System.currentTimeMillis - deleteDelayMS)
          val (pendingEntries, toKill) =
            using(conn.prepareStatement("SELECT id, cache_id FROM pending_deletes WHERE delete_queued < ?")) { stmt =>
              stmt.setTimestamp(1, hardDeleteCutoff)
              using(stmt.executeQuery()) { rs =>
                val pendingEntries = Set.newBuilder[Long]
                val cacheToDelete = Set.newBuilder[Long]
                while(rs.next()) {
                  pendingEntries += rs.getLong(1)
                  cacheToDelete += rs.getLong(2)
                }
                (pendingEntries.result(), cacheToDelete.result())
              }
            }

          log.info("Found {} pending deletions old enough to actually delete", pendingEntries.size)
          if(pendingEntries.nonEmpty) {
            val pendingDeletesHandled = multiDelete(conn, "pending_deletes", "id", pendingEntries)
            val cacheDataDeleted = multiDelete(conn, "cache_data", "cache_id", toKill)
            val cacheEntriesDeleted = multiDelete(conn, "cache", "id", toKill)
            log.info("Deleted {} cache data lines in {} entries", cacheDataDeleted, cacheEntriesDeleted)

            log.info("Handled {} pending deletes", pendingDeletesHandled)
          }

          // ok, now we need to kill any cache entries with a NULL key and an old enough created_at and which are NOT in pending_deletes
          val uncreated = using(conn.prepareStatement("SELECT id FROM cache WHERE key IS NULL and created_at < ? AND id NOT IN (SELECT cache_id FROM pending_deletes)")) { stmt =>
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis - assumeDeadCreateCutoffMS))
            using(stmt.executeQuery()) { rs =>
              val res = Set.newBuilder[Long]
              while(rs.next()) res += rs.getLong(1)
              res.result()
            }
          }
          log.info("Found {} not-fully-created cache entries", uncreated.size)
          if(uncreated.nonEmpty) {
            using(conn.createStatement()) { stmt =>
              stmt.execute(uncreated.mkString("DELETE FROM cache_data WHERE cache_id IN (",",",")"))
              val cacheDataDeleted = stmt.getUpdateCount
              stmt.execute(uncreated.mkString("DELETE FROM cache WHERE id IN (",",",")"))
              val cacheEntriesDeleted = stmt.getUpdateCount

              log.info("Deleted {} cache data lines in {} uncreated entries", cacheDataDeleted, cacheEntriesDeleted)
            }
          }

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

      while(!stage1()) {}

      // ok, we've deleted everything that's definitively dead
      // so now let's kill things that have expired

      def stage2(): Boolean = {
        val oldIsolation = conn.getTransactionIsolation
        try {
          conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
          val deletedCacheEntries =
            using(conn.prepareStatement("INSERT INTO pending_deletes (cache_id, delete_queued) SELECT id, now() FROM cache WHERE key IS NOT NULL AND approx_last_access < ? RETURNING cache_id")) { stmt =>
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
          log.info("Added {} entries to the pending delete queue", deletedCacheEntries.size)
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
}

package com.socrata.querycoordinator.caching.cache.postgresql

import java.io.OutputStream
import java.sql.{Connection, SQLException, Timestamp}
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

import com.mchange.v2.resourcepool.TimeoutException
import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.cache.{CacheSession, ValueRef}
import com.socrata.querycoordinator.caching.{ChunkingOutputStream, CloseBlockingOutputStream}
import com.socrata.util.io.StreamWrapper
import org.postgresql.util.PSQLException

import scala.concurrent.duration.FiniteDuration

class PostgresqlCacheSession(ds: DataSource, dataset: String, updateATimeInterval: FiniteDuration, streamWrapper: StreamWrapper) extends CacheSession {

  import PostgresqlCacheSchema._
  val timeout = FiniteDuration(20, TimeUnit.SECONDS)

  private val updateATimeMS = updateATimeInterval.toMillis
  private val scope = new ResourceScope
  private var closed = false
  @volatile private var readConnection: Connection = null
  @volatile private var writeConnection: Connection = null
  private var updateOnClose = Set.empty[Long]

  private def initRead(): Boolean = {
    if(closed) throw new IllegalStateException("Accessing a closed session")
    if(readConnection eq null) {
      readConnection =
        try {
          scope.open(ds.getConnection)
        }
        catch {
          case e: SQLException if e.getCause.isInstanceOf[TimeoutException] => return false
        }
      readConnection.setReadOnly(true)
      readConnection.setAutoCommit(true)
      using(readConnection.createStatement()) { stmt =>
        stmt.execute("SET statement_timeout TO 20000") // 20 seconds
      }
    }
    true
  }

  private def initWrite(): Boolean = {
    if(closed) throw new IllegalStateException("Accessing a closed session")
    if(writeConnection eq null) {
      writeConnection =
        try { scope.open(ds.getConnection) }
        catch { case e: SQLException if e.getCause.isInstanceOf[TimeoutException] => return false }
      writeConnection.setReadOnly(false)
      writeConnection.setAutoCommit(false)
      using (writeConnection.createStatement()) { stmt =>
        stmt.execute("SET statement_timeout TO 60000") // 1 minutes
      }
    }
    true
  }

  override def find(key: String, resourceScope: ResourceScope): CacheSession.Result[Option[ValueRef]] = synchronized {
    if(!initRead()) return CacheSession.Timeout

    try {
      ensureCacheData(dataset, readConnection) {
        using(readConnection.prepareStatement(s"select id, approx_last_access from ${table} where key = ?")) { stmt =>
          setTimeout(readConnection, timeout)
          stmt.setString(1, key)
          using(stmt.executeQuery()) { rs =>
            if (rs.next()) {
              val dataKey = rs.getLong(1)
              val approx_last_access = rs.getTimestamp(2)

              if (approx_last_access.getTime < System.currentTimeMillis - updateATimeMS) updateOnClose += dataKey

              CacheSession.Success(Some(resourceScope.open(new PostgresqlValueRef(readConnection, dataset, dataKey, scope, streamWrapper))))
            } else {
              CacheSession.Success(None)
            }
          }
        }
      }
    } catch {
      case ex: PSQLException if "57014" == ex.getSQLState =>
        CacheSession.Timeout
    }
  }

  override def create(key: String)(filler: CacheSession.Result[OutputStream] => Unit): Unit = synchronized {
    if(!initWrite()) { filler(CacheSession.Timeout); return }

    try {
      val cacheId =
        ensureCacheData(dataset, writeConnection) {
          val id =
            using(writeConnection.prepareStatement(s"INSERT INTO ${table} (key, created_at, approx_last_access, dataset) VALUES (NULL, ?, ?, ?) RETURNING id")) { stmt =>
              val now = new Timestamp(System.currentTimeMillis())
              stmt.setTimestamp(1, now)
              stmt.setTimestamp(2, now)
              stmt.setString(3, dataset)
              using(stmt.executeQuery()) { rs =>
                if (!rs.next()) sys.error("insert..returning did not return anything?")
                rs.getLong(1)
              }
            }

          using(writeConnection.prepareStatement(s"INSERT INTO ${dataTable(dataset)} (cache_id, chunknum, data) VALUES (?, ?, ?)")) { stmt =>
            val os = new ChunkingOutputStream(65536) {
              var chunkNum = 0

              def onChunk(bytes: Array[Byte]): Unit = {
                stmt.setLong(1, id)
                stmt.setInt(2, chunkNum)
                chunkNum += 1
                stmt.setBytes(3, bytes)
                stmt.execute()
              }
            }
            using(new ResourceScope) { rs =>
              val stream = streamWrapper.wrapOutputStream(rs.open(new CloseBlockingOutputStream(os)), rs)
              filler(CacheSession.Success(stream))
            }
            os.close()
          }
          id
        }

      writeConnection.commit() // ok, we have created (but not linked in) our entry

      def createLink(): Boolean = {
        val oldIsolationLevel = writeConnection.getTransactionIsolation
        try {
          writeConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)

          val oldRow =
            using(writeConnection.prepareStatement(s"SELECT id FROM ${table} WHERE key = ? FOR UPDATE")) { stmt =>
              stmt.setString(1, key)
              using(stmt.executeQuery()) { rs =>
                if(rs.next()) {
                  Some(rs.getLong(1))
                } else {
                  None
                }
              }
            }

          oldRow.foreach { oldId =>
            using(writeConnection.prepareStatement(s"UPDATE ${table} SET key = null WHERE id = ?")) { stmt =>
              stmt.setLong(1, oldId)
              stmt.execute()
            }

            using(writeConnection.prepareStatement(s"INSERT INTO pending_deletes (cache_id, delete_queued, dataset) VALUES (?, ?, ?)")) { stmt =>
              stmt.setLong(1, oldId)
              stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()))
              stmt.setString(3, dataset)
              stmt.execute()
            }
            oldId
          }

          using(writeConnection.prepareStatement(s"UPDATE ${table} SET key = ? WHERE id = ?")) { stmt =>
            stmt.setString(1, key)
            stmt.setLong(2, cacheId)
            stmt.execute()
          }

          writeConnection.commit()
          true
        } catch {
          case e: SQLException if e.getSQLState == "40001" /* serialization_failure */ =>
            false
        } finally {
          writeConnection.rollback()
          writeConnection.setTransactionIsolation(oldIsolationLevel)
        }
      }

      while(!createLink()) {}
    } finally {
      writeConnection.rollback()
    }
  }

  def close(): Unit = synchronized {
    if(!closed) {
      closed = true
      scope.close()
      if(updateOnClose.nonEmpty) {
        using(ds.getConnection()) { conn =>
          conn.setAutoCommit(true)
          conn.setReadOnly(false)
          using(conn.createStatement()) { stmt =>
            stmt.execute(updateOnClose.mkString(s"UPDATE ${table} SET approx_last_access = NOW() WHERE id IN (",",",")"))
          }
        }
      }
    }
  }

  def closeAbnormally(): Unit = synchronized {
    if(!closed) {
      closed = true
      scope.close()
    }
  }

  def ensureCacheData[T](dataset: String, conn: Connection)(t: => T): T = {
    try {
      t
    } catch {
      case ex: PSQLException if "42P01" == ex.getSQLState =>
        if (conn.getAutoCommit) {
          conn.clearWarnings()
        } else {
          conn.rollback()
        }
        PostgresqlCacheSchema.create(ds, Some(dataset))
        t
    }
  }
}

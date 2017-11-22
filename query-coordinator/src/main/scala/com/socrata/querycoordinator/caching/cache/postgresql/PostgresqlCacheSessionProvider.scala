package com.socrata.querycoordinator.caching.cache.postgresql

import javax.sql.DataSource

import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.cache._
import com.socrata.util.io.StreamWrapper
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration

class PostgresqlCacheSessionProvider(ds: DataSource,
                                     updateATimeInterval: FiniteDuration,
                                     survivorCutoff: FiniteDuration,
                                     deleteDelay: FiniteDuration,
                                     assumeDeadCreateCutoff: FiniteDuration,
                                     deleteChunkSize: Int,
                                     useBatchDelete: () => Boolean,
                                     streamWrapper: StreamWrapper = StreamWrapper.noop,
                                     minimumQueryTimeMs: Long)
  extends CacheSessionProvider with CacheCleanerProvider with CacheInit
{
  protected val log: Logger = org.slf4j.LoggerFactory.getLogger(classOf[PostgresqlCacheSessionProvider])

  override val minQueryTimeMs: Long = minimumQueryTimeMs

  private object JdbcCacheSessionResource extends Resource[PostgresqlCacheSession] {
    override def close(a: PostgresqlCacheSession): Unit = a.close()
    override def closeAbnormally(a: PostgresqlCacheSession, e: Throwable) = a.closeAbnormally()
  }

  override def init(): Unit = {
    PostgresqlCacheSchema.create(ds, None)
  }

  override def open(rs: ResourceScope, dataset: String): CacheSession =
    rs.open(new PostgresqlCacheSession(ds, dataset.replace(".", "_"), updateATimeInterval, streamWrapper))(JdbcCacheSessionResource)

  def cleaner(id: String, interval: FiniteDuration): CacheCleaner =
    new PostgresqlCacheCleaner(ds, survivorCutoff, deleteDelay, assumeDeadCreateCutoff, deleteChunkSize, useBatchDelete, interval, id)
}

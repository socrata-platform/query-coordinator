package com.socrata.querycoordinator.caching.cache.file

import java.io.File

import com.rojoma.simplearm.v2._
import com.socrata.querycoordinator.caching.cache._
import com.socrata.util.io.StreamWrapper

import scala.concurrent.duration.FiniteDuration

/**
 * n.b., this does NOT create the directory until/unless `init` is called
 */
class FileCacheSessionProvider(dir: File,
                               updateATimeInterval: FiniteDuration,
                               survivorCutoff: FiniteDuration,
                               assumeDeadCreateCutoff: FiniteDuration,
                               streamWrapper: StreamWrapper = StreamWrapper.noop)
  extends CacheSessionProvider with CacheCleanerProvider with CacheInit
{
  protected val log = org.slf4j.LoggerFactory.getLogger(classOf[FileCacheSessionProvider])

  private object FileCacheSessionResource extends Resource[FileCacheSession] {
    override def close(a: FileCacheSession): Unit = a.close()
  }

  override def init(): Unit = {
    dir.mkdirs()
  }

  override def open(rs: ResourceScope, dataset: String): CacheSession =
    rs.open(new FileCacheSession(dir, updateATimeInterval, streamWrapper))(FileCacheSessionResource)

  override def cleaner(id: String, interval: FiniteDuration): CacheCleaner =
    new FileCacheCleaner(dir, survivorCutoff, assumeDeadCreateCutoff)
}

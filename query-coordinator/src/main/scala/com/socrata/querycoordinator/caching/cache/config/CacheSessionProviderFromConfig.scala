package com.socrata.querycoordinator.caching.cache.config

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.rojoma.simplearm.v2._
import com.socrata.util.io.StreamWrapper
import com.socrata.querycoordinator.caching.cache.noop.NoopCacheSessionProvider
import com.socrata.querycoordinator.caching.cache.{CacheInit, CacheCleanerProvider, CacheSessionProvider}
import com.socrata.querycoordinator.caching.cache.file.FileCacheSessionProvider
import com.socrata.querycoordinator.caching.cache.postgresql.PostgresqlCacheSessionProvider
import org.postgresql.ds.PGSimpleDataSource

object CacheSessionProviderFromConfig {
  private implicit object DSResource extends Resource[ComboPooledDataSource] {
    def close(ds: ComboPooledDataSource) = ds.close()
  }

  def apply(rs: ResourceScope, config: CacheConfig, useBatchDelete: () => Boolean, streamWrapper: StreamWrapper = StreamWrapper.noop): CacheSessionProvider with CacheCleanerProvider with CacheInit =
    config.realConfig match {
      case fs: FilesystemCacheConfig =>
        fromFilesystem(rs, fs, streamWrapper)
      case pg: PostgresqlCacheConfig =>
        fromPostgres(rs, pg, useBatchDelete, streamWrapper, config.minQueryTime)
      case NoopCacheConfig =>
        rs.openUnmanaged(new NoopCacheSessionProvider)
    }

  def fromFilesystem(rs: ResourceScope, fs: FilesystemCacheConfig, streamWrapper: StreamWrapper) =
    rs.openUnmanaged(new FileCacheSessionProvider(
      fs.root,
      updateATimeInterval = fs.atimeUpdateInterval,
      survivorCutoff = fs.survivorCutoff,
      assumeDeadCreateCutoff = fs.assumeDeadCreateCutoff,
      streamWrapper = streamWrapper))

  def fromPostgres(rs: ResourceScope, pg: PostgresqlCacheConfig, useBatchDelete: () => Boolean, streamWrapper: StreamWrapper, minQueryTimeMs: Long) = {
    val ds = rs.open(new ComboPooledDataSource(false))
    ds.setDriverClass("org.postgresql.Driver")
    ds.setJdbcUrl("jdbc:postgresql://" + pg.db.host + ":" + pg.db.port + "/" + pg.db.database)
    ds.setUser(pg.db.username)
    ds.setPassword(pg.db.password)
    ds.setCheckoutTimeout(500) // since we only use this for caching, we can have a brief timeout -- if we can't get one, we'll just not cache
    ds.setTestConnectionOnCheckin(true)
    ds.setPreferredTestQuery("SELECT 1")
    ds.setInitialPoolSize(pg.db.minPoolSize)
    ds.setMinPoolSize(pg.db.minPoolSize)
    ds.setMaxPoolSize(pg.db.maxPoolSize)
    rs.openUnmanaged(
      new PostgresqlCacheSessionProvider(
        ds,
        updateATimeInterval = pg.atimeUpdateInterval,
        survivorCutoff = pg.survivorCutoff,
        assumeDeadCreateCutoff = pg.assumeDeadCreateCutoff,
        deleteDelay = pg.deleteDelay,
        deleteChunkSize = pg.deleteChunkSize,
        useBatchDelete = useBatchDelete,
        streamWrapper = streamWrapper,
        minimumQueryTimeMs = minQueryTimeMs
      ),
      transitiveClose = List(ds)
    )
  }
}

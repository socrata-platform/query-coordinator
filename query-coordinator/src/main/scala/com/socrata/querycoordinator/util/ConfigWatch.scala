package com.socrata.querycoordinator.util

import java.io.Closeable
import java.nio.charset.StandardCharsets

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.util.JsonUtil
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCacheListener, NodeCache}
import org.slf4j.LoggerFactory

class ConfigWatch[T : JsonDecode](curator: CuratorFramework, path: String, default: T) extends (() => T) with Closeable { self =>
  private val log = LoggerFactory.getLogger(classOf[ConfigWatch[_]])

  @volatile private var currentValue = Option.empty[T]

  private val cache = new NodeCache(curator, path)
  private val listener = new NodeCacheListener {
    override def nodeChanged(): Unit = self.synchronized { currentValue = None }
  }
  cache.getListenable.addListener(listener)

  def start(): Unit = {
    cache.start(true)
  }

  override def close(): Unit = {
    cache.close()
  }

  override def apply(): T = {
    val v = currentValue
    v match {
      case Some(t) =>
        t
      case None =>
        self.synchronized {
          currentValue match {
            case Some(t) =>
              t
            case None =>
              val newData =
                for {
                  data <- Option(cache.getCurrentData)
                  bytes <- Option(data.getData)
                } yield {
                  try {
                    JsonUtil.parseJson[T](new String(bytes, StandardCharsets.UTF_8)) match {
                      case Right(t) =>
                        Some(t)
                      case Left(error) =>
                        log.warn("Undecodable data in config watch {}: {}", path: Any, error.english: Any)
                        None
                    }
                  } catch {
                    case e: Exception =>
                      log.warn("Unparsable data in config watch {}: {}", path: Any, e: Any)
                      None
                  }
                }
              val result = newData.flatten.getOrElse(default)
              currentValue = Some(result)
              result
          }
        }
    }
  }
}

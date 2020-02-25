package org.ekstep.dp.functions

import org.apache.spark.broadcast.Broadcast
import org.ekstep.dp.cache.{DedupEngine, RedisConnect}
import org.ekstep.dp.domain.Event
import org.ekstep.dp.task.{PipelinePreprocessorSparkConfig, KafkaSink}
import org.slf4j.LoggerFactory

class DeduplicationFunction(config: PipelinePreprocessorSparkConfig) extends java.io.Serializable {

  private[this] val logger = LoggerFactory.getLogger(classOf[DeduplicationFunction])
  lazy val redisConnect = new RedisConnect(config)
  lazy val dedupEngine = new DedupEngine(redisConnect, config.dedupStore, config.cacheExpirySeconds)

  def checkDuplicate(event: Event)(implicit kafkaSink: Broadcast[KafkaSink]): Option[Event] = {
    if(isDuplicateCheckRequired(event)) {
      if (!dedupEngine.isUniqueEvent(event.mid())) {
        logger.info(s"Duplicate Event mid: ${event.mid}")
        event.markDuplicate()
        kafkaSink.value.send(config.kafkaDuplicateTopic, event.getJson)
        None
      } else {
        logger.info(s"Adding mid: ${event.mid} to Redis")
        dedupEngine.storeChecksum(event.mid)
        // event.markSuccess()
        // kafkaSink.value.send(config.kafkaSuccessTopic, event.getJson)
        Some(event)
      }
    } else {
      // event.markSuccess()
      // kafkaSink.value.send(config.kafkaSuccessTopic, event.getJson)
      Some(event)
    }
  }

  def isDuplicateCheckRequired(event: Event): Boolean = {
    config.includedProducersForDedup.contains(event.producerId())
  }
}

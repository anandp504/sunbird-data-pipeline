package org.ekstep.dp.task

import org.ekstep.dp.core.BaseJobConfig

import scala.collection.JavaConverters._

class DeduplicationConfig extends BaseJobConfig {

  private val serialVersionUID = 2905979434303791379L

  // val sparkTaskMaster: String = config.getString("task.master")
  val sparkMicroBatchingInterval: Int = config.getInt("task.microbatch.interval")

  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")
  val kafkaMalformedTopic: String = config.getString("kafka.output.malformed.topic")
  val includedProducersForDedup: List[String] = config.getStringList("dedup.producer.included.ids").asScala.toList

}

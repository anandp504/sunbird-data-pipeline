package org.sunbird.dp.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.dp.core.BaseJobConfig
import org.sunbird.dp.domain.Event

import scala.collection.JavaConverters._

class PipelinePreprocessorConfig(override val config: Config) extends BaseJobConfig(config) {

  private val serialVersionUID = 2905979434303791379L

  implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val schemaPath: String = config.getString("telemetry.schema.path")

  val dedupStore: Int = config.getInt("redis.database.duplicationstore.id")
  val cacheExpirySeconds: Int = config.getInt("redis.database.key.expiry.seconds")

  // Kafka Topic Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")

  val kafkaPrimaryRouteTopic: String = config.getString("kafka.output.primary.route.topic")
  val kafkaSecondaryRouteTopic: String = config.getString("kafka.output.secondary.route.topic")

  val kafkaAuditRouteTopic: String = config.getString("kafka.output.audit.route.topic")

  val kafkaFailedTopic: String = config.getString("kafka.output.failed.topic")
  val kafkaMalformedTopic: String = config.getString("kafka.output.malformed.topic")
  val kafkaDuplicateTopic: String = config.getString("kafka.output.duplicate.topic")

  val secondaryRouteEids: List[String] = config.getStringList("router.secondary.routes.eid").asScala.toList

  val defaultChannel: String = config.getString("default.channel")

  val includedProducersForDedup: List[String] = config.getStringList("dedup.producer.included.ids").asScala.toList

  // Validation & dedup Stream out put tag
  val validationFailedEventsOutputTag: OutputTag[Event] = OutputTag[Event]("validation-failed-events")
  val uniqueEventsOutputTag: OutputTag[Event] = OutputTag[Event]("unique-events")
  val duplicateEventsOutputTag: OutputTag[Event] = OutputTag[Event]("duplicate-events")

  // Router stream out put tags
  val primaryRouteEventsOutputTag: OutputTag[Event] = OutputTag[Event]("primary-route-events")
  val secondaryRouteEventsOutputTag: OutputTag[Event] = OutputTag[Event]("secondary-route-events")
  val auditRouteEventsOutputTag: OutputTag[Event] = OutputTag[Event]("audit-route-events")

  // Share events out put tags
  val shareRouteEventsOutputTag: OutputTag[Event] = OutputTag[Event]("share-route-events")
  val shareItemEventOutTag: OutputTag[String] = OutputTag[String]("share-route-events")

  val job_name = "PipelinePreprocessorStreamJob"

}

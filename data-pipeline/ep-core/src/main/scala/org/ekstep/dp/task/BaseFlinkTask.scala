package org.ekstep.dp.task

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
import org.ekstep.dp.core.BaseFlinkConfig
import org.ekstep.ep.samza.events.domain.Events

trait BaseFlinkTask[T <: Events] {

  def createStreamProducer(): FlinkKafkaProducer[T] = {

    new FlinkKafkaProducer(BaseFlinkConfig.taskOutputSuccessTopic,
      new ProducerStringSerializationSchema[T](BaseFlinkConfig.taskOutputSuccessTopic),
      BaseFlinkConfig.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

  def createStreamConsumer(): FlinkKafkaConsumer[String] = {
    new FlinkKafkaConsumer[String](BaseFlinkConfig.taskInputTopics, new SimpleStringSchema(),
      BaseFlinkConfig.kafkaConsumerProperties)
  }

}

class ProducerStringSerializationSchema[T <: Events](topic: String) extends KafkaSerializationSchema[T] {
  override def serialize(element: T, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getJson.getBytes(StandardCharsets.UTF_8))
  }
}

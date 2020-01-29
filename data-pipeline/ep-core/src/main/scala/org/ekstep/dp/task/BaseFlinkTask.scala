package org.ekstep.dp.task

import java.nio.charset.StandardCharsets
import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.ekstep.dp.core.BaseJobConfig

abstract class BaseFlinkTask(config: BaseJobConfig) extends Serializable {

  def createStreamConsumer(kafkaTopic: String): FlinkKafkaConsumer[util.Map[String, AnyRef]] = {
    new FlinkKafkaConsumer[util.Map[String, AnyRef]](kafkaTopic, new ConsumerStringDeserializationSchema, config.kafkaConsumerProperties)
  }

  def createStreamProducer(kafkaTopic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer(kafkaTopic,
      new ProducerStringSerializationSchema(kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

}


class ConsumerStringDeserializationSchema extends KafkaDeserializationSchema[util.Map[String, AnyRef]] {

  private val serialVersionUID = -3224825136576915426L

  override def isEndOfStream(nextElement: util.Map[String, AnyRef]): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): util.Map[String, AnyRef] = {
    val parsedString = new String(record.value(), StandardCharsets.UTF_8)
    val gson: Gson = new Gson()
    gson.fromJson(parsedString, new util.HashMap[String, AnyRef]().getClass)
  }

  override def getProducedType: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
}

class ProducerStringSerializationSchema(topic: String) extends KafkaSerializationSchema[String] {

  private val serialVersionUID = -4284080856874185929L

  override def serialize(element: String, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    // implicit val formats: DefaultFormats = DefaultFormats
    new ProducerRecord[Array[Byte], Array[Byte]](topic, element.getBytes(StandardCharsets.UTF_8))
  }
}



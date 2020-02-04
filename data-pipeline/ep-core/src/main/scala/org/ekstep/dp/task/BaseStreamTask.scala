package org.ekstep.dp.task

import java.nio.charset.StandardCharsets
import java.util

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.ekstep.dp.core.BaseJobConfig

import scala.collection.JavaConverters._

abstract class BaseStreamTask(config: BaseJobConfig) extends Serializable {

  def createStreamConsumer(kafkaTopic: String): FlinkKafkaConsumer[util.Map[String, AnyRef]] = {
    new FlinkKafkaConsumer[util.Map[String, AnyRef]](kafkaTopic, new ConsumerStringDeserializationSchema, config.kafkaConsumerProperties)
  }

  def createStreamProducer(kafkaTopic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer(kafkaTopic,
      new ProducerStringSerializationSchema(kafkaTopic), config.kafkaProducerProperties, Semantic.AT_LEAST_ONCE)
  }

  def createSparkStreamConsumer(kafkaTopic: String)(implicit streamingContext: StreamingContext)
  : InputDStream[ConsumerRecord[String, util.Map[String, AnyRef]]] = {
    KafkaUtils.createDirectStream[String, util.Map[String, AnyRef]](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, util.Map[String, AnyRef]](Set(kafkaTopic), config.sparkKafkConsumerProperties())
    )
  }

  def createSparkStreamProducer(): KafkaSink = {
    def producerFunction = () => {
      val producer = new KafkaProducer[String, String](config.sparkKafkaProducerProperties().asJava)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(producerFunction)
  }

}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  private val serialVersionUID = 8884674334138965689L
  lazy val producer = createProducer()
  def send(kafkaTopic: String, data: String) = producer.send(new ProducerRecord(kafkaTopic, data))
}

class SparkConsumerDeserializer extends Deserializer[util.Map[String, AnyRef]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): util.Map[String, AnyRef] = {
    val parsedString = new String(data, StandardCharsets.UTF_8)
    val gson: Gson = new Gson()
    gson.fromJson(parsedString, new util.HashMap[String, AnyRef]().getClass)
  }

  override def close(): Unit = {}
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



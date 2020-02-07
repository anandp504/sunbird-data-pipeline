package org.ekstep.dp.core

import java.util.Properties
import java.io.Serializable

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.ekstep.dp.task.SparkConsumerDeserializer

trait BaseJobConfig extends Serializable {

  val config: Config = ConfigFactory.load()
  val kafkaBrokerServers: String = config.getString("kafka.broker-servers")
  val zookeeper: String = config.getString("kafka.zookeeper")
  val groupId: String = config.getString("kafka.groupId")
  val checkpointingInterval: Int = config.getInt("task.checkpointing.interval")

  val parallelism: Int = config.getInt("task.parallelism")

  def kafkaConsumerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokerServers)
    properties.setProperty("group.id", groupId)
    // properties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, new Integer(524288))
    properties
  }

  def kafkaProducerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerServers)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(10))
    // properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, new Integer(67108864))
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, new Integer(16384 * 4))
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties
  }

  def sparkKafkConsumerProperties(): Map[String, AnyRef] = {
    Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokerServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[SparkConsumerDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
  }

  def sparkKafkaProducerProperties(): Map[String, AnyRef] = {
    Map[String, AnyRef](
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokerServers,
      ProducerConfig.LINGER_MS_CONFIG -> new Integer(10),
      ProducerConfig.BATCH_SIZE_CONFIG -> new Integer(16384 * 4),
      ProducerConfig.COMPRESSION_TYPE_CONFIG -> "snappy"
    )
  }

}

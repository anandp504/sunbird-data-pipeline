package org.ekstep.dp.core

import java.util.Properties
import java.io.Serializable

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerConfig

trait BaseJobConfig extends Serializable {

  val config: Config = ConfigFactory.load()
  val kafkaBrokerServers: String = config.getString("kafka.broker-servers")
  val zookeeper: String = config.getString("kafka.zookeeper")
  val groupId: String = config.getString("kafka.groupId")
  val taskInputTopics: String = config.getString("kafka.input.topics")
  val taskOutputSuccessTopic: String = config.getString("kafka.output.success.topic")
  val checkpointingInterval: Int = config.getInt("flink.checkpointing.interval")

  val flinkParallelism: Int = config.getInt("flink.parallelism")

  def kafkaConsumerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaBrokerServers)
    properties.setProperty("group.id", groupId)
    properties
  }

  def kafkaProducerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerServers)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(5))
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, new Integer(16384 * 2))
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties
  }

}

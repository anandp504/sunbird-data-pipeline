package org.ekstep.dp.core

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.ProducerConfig

object BaseFlinkConfig {

  val config: Config = ConfigFactory.load()
  val kafkaBrokerServers: String = config.getString("kafka.broker-servers")
  val zookeeper: String = config.getString("kafka.zookeeper")
  val groupId: String = config.getString("kafka.groupId")
  val taskInputTopics: String = config.getString("kafka.input.topics")
  val taskOutputSuccessTopic: String = config.getString("kafka.output.success.topic")

  val kafkaConsumerProperties = new Properties()
  kafkaConsumerProperties.setProperty("bootstrap.servers", kafkaBrokerServers)
  // kafkaConsumerProperties.setProperty("zookeeper.connect", zookeeper)
  kafkaConsumerProperties.setProperty("group.id", groupId)

  val kafkaProducerProperties = new Properties()
  kafkaProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerServers)

  val flinkParallelism: Int = config.getInt("flink.parallelism")

}

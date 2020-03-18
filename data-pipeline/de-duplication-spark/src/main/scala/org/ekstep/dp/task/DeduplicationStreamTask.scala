package org.ekstep.dp.task

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ekstep.dp.domain.Event
import org.ekstep.dp.functions.DeduplicationFunction

class DeduplicationStreamTask(config: DeduplicationConfig) extends BaseStreamTask(config) {

  private val serialVersionUID = 146697324640926024L

  def process() = {

    val sparkConf = new SparkConf().setAppName("DeduplicationSparkTask")
    implicit val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(config.sparkMicroBatchingInterval))

    try {
      val kafkaConsumerStream = createSparkStreamConsumer(config.kafkaInputTopic)
      // kafkaConsumerStream.foreachRDD(rdd => rdd.map(record => println(record.value())))

      implicit val kafkaSink: Broadcast[KafkaSink]
        = streamingContext.sparkContext.broadcast(createSparkStreamProducer())
      val duplicateMonitor = new DeduplicationFunction(config)

      // kafkaConsumerStream.repartition(config.taskParallelism).foreachRDD {
      kafkaConsumerStream.foreachRDD {
        rdd => rdd.foreach {
            message =>
              val event = new Event(message.value())
              duplicateMonitor.checkDuplicate(event)(kafkaSink)
          }
      }

      streamingContext.start()
      streamingContext.awaitTermination()

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

  def isDuplicateCheckRequired(event: Event): Boolean = {
    config.includedProducersForDedup.contains(event.producerId())
  }

}

object DeduplicationStreamTask {
  val config = new DeduplicationConfig
  def apply(): DeduplicationStreamTask = new DeduplicationStreamTask(config)
  def main(args: Array[String]): Unit = {
    DeduplicationStreamTask.apply().process()
  }
}

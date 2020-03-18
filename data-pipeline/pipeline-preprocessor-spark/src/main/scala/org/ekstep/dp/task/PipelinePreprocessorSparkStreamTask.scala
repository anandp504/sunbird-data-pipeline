package org.ekstep.dp.task

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.ekstep.dp.domain.Event
import org.ekstep.dp.functions.{DeduplicationFunction, TelemetryRouterFunction, TelemetryValidationFunction}

class PipelinePreprocessorSparkStreamTask(config: PipelinePreprocessorSparkConfig) extends BaseStreamTask(config) {

  private val serialVersionUID = 146697324640926024L

  def process(): Unit = {

    val sparkConf = new SparkConf().setAppName("PipelineProcessorSpark")
    implicit val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(config.sparkMicroBatchingInterval))

    try {
      val kafkaConsumerStream = createSparkStreamConsumer(config.kafkaInputTopic)

      implicit val kafkaSink: Broadcast[KafkaSink]
        = streamingContext.sparkContext.broadcast(createSparkStreamProducer())
      val dedupFunction = new DeduplicationFunction(config)
      val telemetryValidationFunction = new TelemetryValidationFunction(config)
      val telemetryRouterFunction = new TelemetryRouterFunction(config)

      kafkaConsumerStream
        .flatMap { event => telemetryValidationFunction.validate(new Event(event.value))(kafkaSink) }
        .flatMap { event => dedupFunction.checkDuplicate(event) }
        .foreachRDD { eventRdd =>
          eventRdd.foreach {
            event => telemetryRouterFunction.route(event)(kafkaSink)
          }
        }

      streamingContext.start()
      streamingContext.awaitTermination()

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}

object PipelinePreprocessorSparkStreamTask {
  val config = new PipelinePreprocessorSparkConfig
  def apply(): PipelinePreprocessorSparkStreamTask = new PipelinePreprocessorSparkStreamTask(config)
  def main(args: Array[String]): Unit = {
    PipelinePreprocessorSparkStreamTask.apply().process()
  }
}

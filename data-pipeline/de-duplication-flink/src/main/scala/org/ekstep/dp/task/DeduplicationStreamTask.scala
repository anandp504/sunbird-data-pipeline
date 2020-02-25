package org.ekstep.dp.task

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.OutputTag
import org.ekstep.dp.functions.DuplicateEventMonitor

class DeduplicationStreamTask(config: DeduplicationConfig) extends BaseStreamTask(config) {

  private val serialVersionUID = 146697324640926024L

  def process() = {

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val v3EventTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    // env.setParallelism(config.parallelism)
    env.enableCheckpointing(config.checkpointingInterval)

    try {
      val kafkaConsumer = createKafkaStreamConsumer(config.kafkaInputTopic)
      kafkaConsumer.setStartFromEarliest()

      val dataStream: SingleOutputStreamOperator[String] =
        env.addSource(kafkaConsumer, "kafka-telemetry-valid-consumer")
          .process(new DuplicateEventMonitor(config))

      /**
        * Separate sinks for duplicate events and unique events
        */
      dataStream.getSideOutput(new OutputTag[String]("unique-event"))
        .addSink(createKafkaStreamProducer(config.kafkaSuccessTopic))
        .name("kafka-telemetry-unique-producer")

      // duplicateStream.getSideOutput(new OutputTag[V3Event]("duplicate-event")).print()
      dataStream.getSideOutput(new OutputTag[String]("duplicate-event"))
        .addSink(createKafkaStreamProducer(config.kafkaDuplicateTopic))
        .name("kafka-telemetry-duplicate-producer")

      env.execute("DeduplicationFlinkJob")

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}

object DeduplicationStreamTask {
  val config = new DeduplicationConfig
  def apply(): DeduplicationStreamTask = new DeduplicationStreamTask(config)
  def main(args: Array[String]): Unit = {
    DeduplicationStreamTask.apply().process()
  }
}

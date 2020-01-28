package org.ekstep.dp.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.OutputTag
import org.ekstep.dp.functions.DuplicateEventMonitor
import org.ekstep.dp.task.BaseFlinkTask

class DeduplicationFlinkTask(config: DeduplicationConfig) extends BaseFlinkTask(config) {

  private val serialVersionUID = 146697324640926024L

  def process() = {

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val v3EventTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    env.enableCheckpointing(config.checkpointingInterval)

    try {
      val kafkaConsumer = createStreamConsumer(config.taskInputTopics)
      kafkaConsumer.setStartFromEarliest()

      val dataStream: SingleOutputStreamOperator[String] =
        env.addSource(kafkaConsumer, "kafka-telemetry-valid-consumer")
          .process(new DuplicateEventMonitor(config))

      dataStream.getSideOutput(new OutputTag[String]("unique-event"))
        .addSink(createStreamProducer(config.taskOutputSuccessTopic))
        .name("kafka-telemetry-unique-producer")

      // duplicateStream.getSideOutput(new OutputTag[V3Event]("duplicate-event")).print()
      dataStream.getSideOutput(new OutputTag[String]("duplicate-event"))
        .addSink(createStreamProducer(config.kafkaDuplicateTopic))
        .name("kafka-telemetry-duplicate-producer")

      env.execute("DeduplicationFlinkJob")
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }

}

object DeduplicationFlinkTask {
  val config = new DeduplicationConfig
  def apply(): DeduplicationFlinkTask = new DeduplicationFlinkTask(config)
  def main(args: Array[String]): Unit = {
    DeduplicationFlinkTask.apply().process()
  }
}

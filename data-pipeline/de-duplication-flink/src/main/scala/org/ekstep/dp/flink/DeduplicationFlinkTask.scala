package org.ekstep.dp.flink

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.ekstep.ep.samza.domain.Event
import java.{util => ju}

import org.ekstep.dp.task.BaseFlinkTask
import org.ekstep.ep.samza.serializer.EventsSerializer


object DeduplicationFlinkTask extends App with BaseFlinkTask[Event] {

  class EventMapProjection extends MapFunction[String, Event] {
    override def map(messageEnvelope: String): Event = {
      val gson = new Gson()
      val mapType = new TypeToken[ju.HashMap[String, Object]] {}.getType
      val jsonMap = gson.fromJson[ju.HashMap[String, Object]](messageEnvelope, mapType)
      println("JSON Map: " + jsonMap)
      new Event(jsonMap)
    }
  }

  override def main(args: Array[String]) = {

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.registerTypeWithKryoSerializer(classOf[Event], classOf[EventsSerializer])

    val kafkaConsumer = createStreamConsumer()
    val dataStream: DataStream[Event] =
      env.addSource(kafkaConsumer, "kafka-telemetry-valid-consumer")
        .map(new EventMapProjection)
    val kafkaProducer = createStreamProducer()
    dataStream.addSink(kafkaProducer).name("kafka-telemetry-unique-producer")
    env.execute("DeduplicationFlinkJob")
  }
}

// class DeduplicationFlinkTask extends ProcessFunction

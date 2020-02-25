package org.ekstep.dp.functions

import org.apache.spark.broadcast.Broadcast
import org.ekstep.dp.domain.Event
import org.ekstep.dp.task.{KafkaSink, PipelinePreprocessorSparkConfig}

class TelemetryRouterFunction(config: PipelinePreprocessorSparkConfig) extends java.io.Serializable {

  private val secondaryRouteEids: List[String] = config.secondaryRouteEids

  def route(event: Event)(implicit kafkaSink: Broadcast[KafkaSink]): Unit = {

    if (secondaryRouteEids.contains(event.eid())) {
      kafkaSink.value.send(config.kafkaSecondaryRouteTopic, event.getJson)
    } else {
      kafkaSink.value.send(config.kafkaPrimaryRouteTopic, event.getJson)
    }

    if ("AUDIT".equalsIgnoreCase(event.eid)) {
      kafkaSink.value.send(config.kafkaAuditRouteTopic, event.getJson)
    }
  }
}

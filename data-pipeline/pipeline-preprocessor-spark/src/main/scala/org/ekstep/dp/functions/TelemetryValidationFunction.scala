package org.ekstep.dp.functions

import org.apache.spark.broadcast.Broadcast
import org.ekstep.dp.domain.Event
import org.ekstep.dp.task.{KafkaSink, PipelinePreprocessorSparkConfig}
import org.ekstep.dp.util.SchemaValidator
import org.slf4j.LoggerFactory

class TelemetryValidationFunction(config: PipelinePreprocessorSparkConfig) extends java.io.Serializable {

  private[this] val logger = LoggerFactory.getLogger(classOf[TelemetryValidationFunction])
  lazy val schemaValidator: SchemaValidator = new SchemaValidator(config)

  def validate(event: Event)(implicit kafkaSink: Broadcast[KafkaSink]): Option[Event] = {

    dataCorrection(event)

    try {
      if (schemaValidator.schemaFileExists(event.schemaName)) {
        val validationReport = schemaValidator.validate(event)
        if (validationReport.isSuccess) {
          event.updateDefaults(config)
          Some(event)
        } else {
          val failedErrorMsg = schemaValidator.getInvalidFieldName(validationReport.toString)
          event.markValidationFailure(failedErrorMsg)
          kafkaSink.value.send(config.kafkaFailedTopic, event.getJson)
          None
        }
      } else {
        logger.info(s"SCHEMA NOT FOUND FOR EID: ${event.eid}")
        logger.debug(s"SKIPPING EVENT ${event.mid} FROM VALIDATION")
        Some(event)
      }
    } catch {
      case ex: Exception =>
        logger.error("Error validating JSON event", ex)
        None
    }

  }

  private def dataCorrection(event: Event): Event = {
    // Remove prefix from federated userIds
    val eventActorId = event.actorId
    if (eventActorId != null && !eventActorId.isEmpty && eventActorId.startsWith("f:"))
      event.updateActorId(eventActorId.substring(eventActorId.lastIndexOf(":") + 1))
    if (event.eid != null && event.eid.equalsIgnoreCase("SEARCH"))
      event.correctDialCodeKey()
    event
  }

}

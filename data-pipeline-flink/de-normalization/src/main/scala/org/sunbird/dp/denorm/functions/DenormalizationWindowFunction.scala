package org.sunbird.dp.denorm.functions

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.slf4j.LoggerFactory
import org.sunbird.dp.core.cache.RedisConnect
import org.sunbird.dp.core.job.{BaseProcessFunction, BaseProcessKeyedFunction, Metrics}
import org.sunbird.dp.denorm.`type`._
import org.sunbird.dp.denorm.domain.Event
import org.sunbird.dp.denorm.task.DenormalizationConfig
import org.sunbird.dp.denorm.util.{DenormCache, DenormWindowCache}

case class WindowEvents(count: Integer, key: String, value: List[Event])

class DenormalizationWindowFunction(config: DenormalizationConfig)(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessKeyedFunction[Integer, Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormalizationFunction])

  private[this] var deviceDenormalization: DeviceDenormalization = _
  private[this] var userDenormalization: UserDenormalization = _
  private[this] var dialcodeDenormalization: DialcodeDenormalization = _
  private[this] var contentDenormalization: ContentDenormalization = _
  private[this] var locationDenormalization: LocationDenormalization = _
  private[this] var denormCache: DenormWindowCache = _

  lazy val state: ValueState[WindowEvents] =
    getRuntimeContext.getState(new ValueStateDescriptor[WindowEvents]("state", classOf[WindowEvents]))

  override def metricsList(): List[String] = {
    List(config.eventsExpired, config.userTotal, config.userCacheHit, config.userCacheMiss,
      config.contentTotal, config.contentCacheHit, config.contentCacheMiss, config.deviceTotal,
      config.deviceCacheHit, config.deviceCacheMiss, config.dialcodeTotal,
      config.dialcodeCacheHit, config.dialcodeCacheMiss,
      config.locTotal, config.locCacheHit, config.locCacheMiss)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    denormCache = new DenormWindowCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config))
    denormCache.init()
    deviceDenormalization = new DeviceDenormalization(config)
    userDenormalization = new UserDenormalization(config)
    dialcodeDenormalization = new DialcodeDenormalization(config)
    contentDenormalization = new ContentDenormalization(config)
    locationDenormalization = new LocationDenormalization(config)
  }

  override def close(): Unit = {
    super.close()
    denormCache.close()
    deviceDenormalization.closeDataCache()
    userDenormalization.closeDataCache()
    dialcodeDenormalization.closeDataCache()
    contentDenormalization.closeDataCache()
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Integer, Event, Event]#OnTimerContext, metrics: Metrics): Unit = {
    Option( state.value ) match {
      case None => // ignore
      case Some( denormEvents ) => {
        // status.value.foreach { denormEvent => denormalize(denormEvent, ctx, metrics) }
        denormalize(denormEvents.value, ctx, metrics)
        state.clear()
      }
    }
  }

  override def processElement(event: Event,
                              context: KeyedProcessFunction[Integer, Event, Event]#Context,
                              metrics: Metrics): Unit = {
    if (event.isOlder(config.ignorePeriodInMonths)) { // Skip events older than configured value (default: 3 months)
      metrics.incCounter(config.eventsExpired)
    } else {
      if ("ME_WORKFLOW_SUMMARY" == event.eid() || !event.eid().contains("SUMMARY")) {
        context.timerService().registerProcessingTimeTimer(context.timestamp() + 5)

        val updated: WindowEvents = Option(state.value) match {
          case None => {
            WindowEvents(1, null, List(event))
          }
          case Some(currentEvent) => WindowEvents(currentEvent.count + 1, null, event :: currentEvent.value)
        }

        if (updated.count == 5) {
          // updated.value.foreach { denormEvent => denormalize(denormEvent, context, metrics) }
          denormalize(updated.value, context, metrics)
          state.clear()
        } else {
          state.update( updated )
        }
      }
    }
  }

  def denormalize(events: List[Event], context: KeyedProcessFunction[Integer, Event, Event]#Context, metrics: Metrics) = {
    val denormData = denormCache.getDenormData(events)
    denormData.foreach {
      eventDenormData =>
        val denormEvent = events.filter(_.mid == eventDenormData.mid).head
        deviceDenormalization.denormalize(denormEvent, eventDenormData, metrics)
        userDenormalization.denormalize(denormEvent, eventDenormData, metrics)
        dialcodeDenormalization.denormalize(denormEvent, eventDenormData, metrics)
        contentDenormalization.denormalize(denormEvent, eventDenormData, metrics)
        locationDenormalization.denormalize(denormEvent, eventDenormData, metrics)
        context.output(config.denormEventsTag, denormEvent)
    }
  }

}

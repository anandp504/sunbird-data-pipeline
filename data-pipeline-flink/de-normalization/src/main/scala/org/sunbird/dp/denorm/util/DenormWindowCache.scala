package org.sunbird.dp.denorm.util

import java.util
import com.google.gson.Gson
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{Map => MMap}
import org.sunbird.dp.core.cache.RedisConnect
import org.sunbird.dp.denorm.domain.Event
import redis.clients.jedis.Pipeline
import org.sunbird.dp.denorm.task.DenormalizationConfig
import org.sunbird.dp.core.domain.EventsPath
import redis.clients.jedis.Response

trait RedisData {
  def content: MMap[String, AnyRef]
  def collection: MMap[String, AnyRef]
  def l2data: MMap[String, AnyRef]
  def device: MMap[String, AnyRef]
  def dialCode: MMap[String, AnyRef]
  def user: MMap[String, AnyRef]
}
case class DenormKey(mid: String, denormType: String)
case class DenormData(mid: String, content: MMap[String, AnyRef], collection: MMap[String, AnyRef], l2data: MMap[String, AnyRef], device: MMap[String, AnyRef],
                      dialCode: MMap[String, AnyRef], user: MMap[String, AnyRef]) extends RedisData

class DenormWindowCache(val config: DenormalizationConfig, val redisConnect: RedisConnect) {

  private[this] val logger = LoggerFactory.getLogger(classOf[DenormCache])
  private var pipeline: Pipeline = _
  val gson = new Gson()

  def init() {
    this.pipeline = redisConnect.getConnection(0).pipelined()
  }

  def close() {
    this.pipeline.close()
  }

  def getDenormData(events: List[Event]): List[DenormData] = {
    this.pipeline.clear()
    val responses = MMap[DenormKey, AnyRef]()
    getContentCache(events, responses)
    getDeviceCache(events, responses)
    getDialcodeCache(events, responses)
    getUserCache(events, responses)
    this.pipeline.sync()
    parseResponses(responses)
  }

  private def getContentCache(events: List[Event], responses: MMap[DenormKey, AnyRef]) {
    this.pipeline.select(config.contentStore)
    events.foreach { event =>
      val objectType = event.objectType()
      val objectId = event.objectID()
      if (!List("user", "qr", "dialcode").contains(objectType) && null != objectId) {
        responses.put(DenormKey(event.mid(), "content"), this.pipeline.get(objectId).asInstanceOf[AnyRef])

        if (event.checkObjectIdNotEqualsRollUpId(EventsPath.OBJECT_ROLLUP_L1)) {
          responses.put(DenormKey(event.mid(), "collection"), this.pipeline.get(event.objectRollUpl1ID()).asInstanceOf[AnyRef])
        }
        if (event.checkObjectIdNotEqualsRollUpId(EventsPath.OBJECT_ROLLUP_L2)) {
          responses.put(DenormKey(event.mid(), "l2data"), this.pipeline.get(event.objectRollUpl2ID()).asInstanceOf[AnyRef])
        }
      }
    }
  }

  private def getDialcodeCache(events: List[Event], responses: MMap[DenormKey, AnyRef]) {
    this.pipeline.select(config.dialcodeStore)
    events.foreach { event =>
      if (null != event.objectType() && List("dialcode", "qr").contains(event.objectType().toLowerCase())) {
        responses.put(DenormKey(event.mid(), "dialcode"), this.pipeline.get(event.objectID().toUpperCase()).asInstanceOf[AnyRef])
      }
    }
  }

  private def getDeviceCache(events: List[Event], responses: MMap[DenormKey, AnyRef]) {
    this.pipeline.select(config.deviceStore)
    events.foreach { event =>
      if (null != event.did() && event.did().nonEmpty) {
        responses.put(DenormKey(event.mid(), "device"), this.pipeline.hgetAll(event.did()).asInstanceOf[AnyRef])
      }
    }
  }

  private def getUserCache(events: List[Event], responses: MMap[DenormKey, AnyRef]) {
    this.pipeline.select(config.userStore)
    events.foreach { event =>
      val actorId = event.actorId()
      val actorType = event.actorType()
      if (null != actorId && actorId.nonEmpty && !"anonymous".equalsIgnoreCase(actorId) && ("user".equalsIgnoreCase(Option(actorType).getOrElse("")) || "ME_WORKFLOW_SUMMARY".equals(event.eid()))) {
        responses.put(DenormKey(event.mid(), "user"), this.pipeline.hgetAll(config.userStoreKeyPrefix + actorId).asInstanceOf[AnyRef])
      }
    }
  }

  private def parseResponses(cacheResponses: MMap[DenormKey, AnyRef]) : List[DenormData] = {

    cacheResponses.groupBy(r => r._1.mid).map {
      case (mid, denormData) =>
        val eventResponse = MMap[String, MMap[String, AnyRef]]()
        denormData.map {
          case (key, value) =>
            key.denormType match {
              case "user" =>
                eventResponse.put("userData", convertToComplexDataTypes(getData(value.asInstanceOf[Response[java.util.Map[String, String]]], config.userFields)))
              case "device" =>
                eventResponse.put("deviceData", convertToComplexDataTypes(getData(value.asInstanceOf[Response[java.util.Map[String, String]]], config.deviceFields)))
              case "content" =>
                eventResponse.put("contentData", getDataMap(value.asInstanceOf[Response[String]], config.contentFields))
              case "collection" =>
                eventResponse.put("collectionData", getDataMap(value.asInstanceOf[Response[String]], config.contentFields))
              case "l2data" =>
                eventResponse.put("l2Data", getDataMap(value.asInstanceOf[Response[String]], config.contentFields))
              case "dialcode" =>
                eventResponse.put("dialcodeData", getDataMap(value.asInstanceOf[Response[String]], config.dialcodeFields))
              case _ => // Do nothing
            }
        }
        DenormData(mid = mid,
          content = eventResponse.getOrElse("contentData", MMap[String, AnyRef]()),
          device = eventResponse.getOrElse("deviceData", MMap[String, AnyRef]()),
          user = eventResponse.getOrElse("userData", MMap[String, AnyRef]()),
          collection = eventResponse.getOrElse("collectionData", MMap[String, AnyRef]()),
          l2data = eventResponse.getOrElse("l2Data", MMap[String, AnyRef]()),
          dialCode = eventResponse.getOrElse("dialcodeData", MMap[String, AnyRef]())
        )
    }.toList
  }

  private def getData(data: Response[java.util.Map[String, String]], fields: List[String]): mutable.Map[String, String] = {
    val dataMap = data.get
    if (dataMap.size() > 0) {
      if (fields.nonEmpty) dataMap.keySet().retainAll(fields.asJava)
      dataMap.values().removeAll(util.Collections.singleton(""))
      dataMap.asScala
    } else {
      mutable.Map[String, String]()
    }
  }

  private def getDataMap(dataStr: Response[String], fields: List[String]): mutable.Map[String, AnyRef] = {
    val data = dataStr.get
    if (data != null && !data.isEmpty) {
      val dataMap = gson.fromJson(data, new util.HashMap[String, AnyRef]().getClass)
      if (fields.nonEmpty) dataMap.keySet().retainAll(fields.asJava)
      dataMap.values().removeAll(util.Collections.singleton(""))
      dataMap.asScala
    } else {
      mutable.Map[String, AnyRef]()
    }
  }

  def isArray(value: String): Boolean = {
    val redisValue = value.trim
    redisValue.length > 0 && redisValue.startsWith("[")
  }

  def isObject(value: String) = {
    val redisValue = value.trim
    redisValue.length > 0 && redisValue.startsWith("{")
  }

  def convertToComplexDataTypes(data: mutable.Map[String, String]): mutable.Map[String, AnyRef] = {
    val result = mutable.Map[String, AnyRef]()
    data.keys.map {
      redisKey =>
        val redisValue = data(redisKey)
        if (isArray(redisValue)) {
          result += redisKey -> gson.fromJson(redisValue, new util.ArrayList[AnyRef]().getClass)
        } else if (isObject(redisValue)) {
          result += redisKey -> gson.fromJson(redisValue, new util.HashMap[String, AnyRef]().getClass)
        } else {
          result += redisKey -> redisValue
        }
    }
    result
  }

}

package org.ekstep.ep.samza.serializer

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.serializers.MapSerializer
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.ekstep.ep.samza.domain.Event

import java.{util => ju}

class EventsSerializer extends Serializer[Event] {
  override def write(kryo: Kryo, output: Output, t: Event): Unit = {
    kryo.writeObject(output, t.getMap, new MapSerializer())
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[Event]): Event = {
    val mapObject = kryo.readObject(input, classOf[ju.Map[String, Object]], new MapSerializer())
    new Event(mapObject)
  }
}

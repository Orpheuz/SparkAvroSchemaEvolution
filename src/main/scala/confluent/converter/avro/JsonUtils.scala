package confluent.converter.avro

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.avro.util.internal.JacksonUtils

import java.io.ByteArrayOutputStream

object JsonUtils {

  private lazy val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  def objectToJsonString(obj: Object): Option[String] = {

    Option(JacksonUtils.toJsonNode(obj))
      .map { json =>
        val baos = new ByteArrayOutputStream()
        objectMapper.writeValue(baos, json)
        baos.toString
      }
  }

  def jsonStringToObject(jsonString: String): Object = {

    val jsonNode = objectMapper.readTree(jsonString)
    JacksonUtils.toObject(jsonNode)
  }

  def jsonStringToMap(jsonString: String): Map[String, AnyRef] = {
    objectMapper.readValue(jsonString, classOf[Map[String, AnyRef]])
  }
}

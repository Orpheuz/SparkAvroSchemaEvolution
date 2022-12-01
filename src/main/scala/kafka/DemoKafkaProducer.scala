package kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DemoKafkaProducer(
  bootstrapServers: String,
  topicName: String,
  registryURL: String,
  registryOptions: Map[String, _] = Map.empty) {

  private val producerProps = new Properties
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://superUser:superUser@localhost:8084")
  producerProps
    .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  producerProps.putAll(registryOptions.asJava)

  private val kafkaProducer = new KafkaProducer[Integer, GenericRecord](producerProps)

  private val schemaRegistryClient = new CachedSchemaRegistryClient(
    registryURL,
    10,
    registryOptions.asJava
  )

  private def registerSchema(subject: String, schema: Schema): Unit = {

    Try(schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema) match {
      case Failure(_) => schemaRegistryClient.register(subject, schema)
      case Success(registeredSchema) if registeredSchema.equals(schema.toString) =>
        schemaRegistryClient.register(subject, schema)
      case Success(_) =>
    }
  }

  private def getSchemaFromResources(fileName: String): String = {

    val file = Source.fromFile(getClass.getResource(fileName).getFile)
    val schema = file.getLines().mkString
    file.close()
    schema
  }

  private def version1(): Unit = {

    val avsc = getSchemaFromResources("/schema1.avsc")
    val parsedSchema = new Schema.Parser().parse(avsc)
    val subject = s"$topicName-value"

    registerSchema(subject, parsedSchema)

    val record = new GenericData.Record(parsedSchema)
    record.put("mandatoryInteger", 1)
    record.put("nullableString", "nullable")

    kafkaProducer.send(new ProducerRecord[Integer, GenericRecord](topicName, record))
  }

  private def version2(): Unit = {

    val avsc = getSchemaFromResources("/schema2.avsc")
    val parsedSchema = new Schema.Parser().parse(avsc)
    val subject = s"$topicName-value"

    registerSchema(subject, parsedSchema)

    val record = new GenericData.Record(parsedSchema)
    record.put("mandatoryInteger", 1)
    record.put("nullableString", "nullable")
    record.put("aNewNullableString", "nullable")

    kafkaProducer.send(new ProducerRecord[Integer, GenericRecord](topicName, record))
  }

  private def version3(): Unit = {

    val avsc = getSchemaFromResources("/schema3.avsc")
    val parsedSchema = new Schema.Parser().parse(avsc)
    val subject = s"$topicName-value"

    registerSchema(subject, parsedSchema)

    val record = new GenericData.Record(parsedSchema)
    record.put("mandatoryInteger", 1)
    record.put("aNewNullableString", "nullable")

    kafkaProducer.send(new ProducerRecord[Integer, GenericRecord](topicName, record))
  }

  def produceNewVersion(version: Int): Unit = {

    version match {
      case 1 => version1()
      case 2 => version2()
      case 3 => version3()
    }
  }
}

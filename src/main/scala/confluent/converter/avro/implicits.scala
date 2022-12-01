package confluent.converter.avro

import confluent.converter.avro.MetadataKeys._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type.{ENUM, UNION}
import org.apache.spark.sql.types.MetadataBuilder

import scala.jdk.CollectionConverters._

object implicits {

  implicit class AvroMetadataBuilder(metadataBuilder: MetadataBuilder = new MetadataBuilder()) {

    def putDoc(field: Schema.Field): MetadataBuilder = {

      JsonUtils.objectToJsonString(field.doc()) match {
        case Some(doc) => metadataBuilder.putString(DOC, doc)
        case None      => metadataBuilder
      }
    }

    def putDefaults(field: Schema.Field): MetadataBuilder = {

      JsonUtils.objectToJsonString(field.defaultVal()) match {
        case Some(defaultJson) => metadataBuilder.putString(DEFAULT, defaultJson)
        case None              =>
      }

      field.schema().getType match {
        case UNION if field.schema().getTypes.asScala.exists(_.getType == ENUM) =>
          enumDefault(field.schema().getTypes.asScala.filter(_.getType == ENUM).head)
        case ENUM => enumDefault(field.schema())
        case _    => metadataBuilder
      }
    }

    private def enumDefault(schema: Schema): MetadataBuilder = {

      Option(schema.getEnumDefault) match {
        case Some(default) =>
          metadataBuilder
            .putStringArray(ENUM_SYMBOLS, schema.getEnumSymbols.asScala.toArray)
            .putString(ENUM_DEFAULT, s""""$default"""")
        case None =>
          metadataBuilder
            .putStringArray(ENUM_SYMBOLS, schema.getEnumSymbols.asScala.toArray)
      }
    }

    def putExtraProps(field: Schema.Field): MetadataBuilder = {

      field.getObjectProps.asScala.foreach { case (key, value) =>
        JsonUtils.objectToJsonString(value) match {
          case Some(prop) => metadataBuilder.putString(key, prop)
          case None       =>
        }
      }
      metadataBuilder
    }

    def putSchemaType(avroType: Schema): MetadataBuilder = {

      metadataBuilder.putString(AVRO_TYPE, avroType.toString)
    }

    def putNullability(isNullable: Boolean): MetadataBuilder = {

      metadataBuilder.putBoolean(NULLABLE, isNullable)
    }
  }
}

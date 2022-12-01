package confluent.converter.avro

import confluent.converter.avro.MetadataKeys._
import org.apache.avro.LogicalTypes.TimestampMillis
import org.apache.avro.Schema.Type._
import org.apache.avro.{JsonProperties, LogicalTypes, Schema, SchemaBuilder}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.Decimal.minBytesForPrecision
import org.apache.spark.sql.types._

import java.util.Objects
import scala.util.Try

object SparkToAvroSchemaConverter {

  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  def toAvroType(
      catalystType: DataType,
      metadata: Metadata = Metadata.empty,
      namespace: String = "",
      nullable: Boolean = false,
      recordName: String = "topLevelRecord"
  ): Schema = {

    def getFixedName(recordName: String, namespace: String): String = {
      // Need to avoid naming conflict for the fixed fields
      namespace match {
        case "" => s"$recordName.fixed"
        case _  => s"$namespace.$recordName.fixed"
      }
    }

    def isNullable(metadata: Metadata): Boolean = {
      Try(metadata.getBoolean(NULLABLE)).toOption match {
        case Some(false) => false
        case _           => true
      }
    }

    def getDefaultValue(metadata: Metadata, defaultKey: String): Option[Object] = {

      val defaultValueOpt = Try(metadata.getString(defaultKey))
        .flatMap(defaultJsonString => {
          Try(JsonUtils.jsonStringToObject(defaultJsonString))
        })
        .toOption
      defaultValueOpt
    }

    val builder = SchemaBuilder.builder()

    val avroSchema = Try(metadata.getString(AVRO_TYPE))
      .toOption
      .map(schema => new Schema.Parser().parse(schema))

    val schema = catalystType match {
      case TimestampType =>
        avroSchema match {
          case Some(schema) if schema.getLogicalType.isInstanceOf[TimestampMillis] =>
            LogicalTypes.timestampMillis().addToSchema(builder.longType())
          case _ => LogicalTypes.timestampMicros().addToSchema(builder.longType())
        }
      case StringType =>
        avroSchema match {
          case Some(schema) if schema.getType.equals(ENUM) =>
            val enum = builder.enumeration(recordName).namespace(namespace)
            val enumWithDefault = getDefaultValue(metadata, ENUM_DEFAULT) match {
              case Some(value) =>
                JsonUtils.objectToJsonString(value) match {
                  case Some(value) => enum.defaultSymbol(value.substring(1, value.length - 1))
                  case None        => enum
                }
              case None => enum
            }
            enumWithDefault.symbols(metadata.getStringArray(ENUM_SYMBOLS): _*)

          case _ =>
            SchemaConverters.toAvroType(catalystType, nullable = false, recordName, namespace)
        }
      case d: DecimalType =>
        avroSchema match {
          case Some(schema) if schema.getType == BYTES =>
            val avroType = LogicalTypes.decimal(d.precision, d.scale)
            avroType.addToSchema(SchemaBuilder.builder().bytesType())
          case _ =>
            val avroType = LogicalTypes.decimal(d.precision, d.scale)
            val name = getFixedName(recordName, namespace)
            val fixedSize = minBytesForPrecision(d.precision)
            val size = avroSchema
              .map { schema =>
                if (schema.getFixedSize > fixedSize) schema.getFixedSize else fixedSize
              }
              .getOrElse {
                fixedSize
              }

            avroType.addToSchema(SchemaBuilder.fixed(name).size(size))
        }
      case BinaryType =>
        avroSchema match {
          case Some(schema) if schema.getType == FIXED =>
            val name = getFixedName(recordName, namespace)
            builder
              .fixed(name)
              .size(schema.getFixedSize)
          case _ => builder.bytesType()
        }
      case ArrayType(et, containsNull) =>
        builder
          .array()
          .items(toAvroType(et, metadata, namespace, containsNull, recordName))
      case MapType(StringType, vt, valueContainsNull) =>
        builder
          .map()
          .values(
            toAvroType(vt, metadata, namespace, valueContainsNull, recordName)
          )
      case st: StructType =>
        val childNameSpace = if (namespace != "") s"$namespace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(namespace).fields()
        st.foreach { f =>
          val fieldAvroType = toAvroType(f.dataType, f.metadata, childNameSpace, f.nullable, f.name)

          val fieldBuilder = fieldsAssembler.name(f.name)

          Try {
            val doc = f.metadata.getString(DOC)
            doc.substring(1, doc.length - 1)
          }.toOption.map(fieldBuilder.doc)

          JsonUtils.jsonStringToMap(f.metadata.json).foreach {
            case (key, _) if RESERVED_SCHEMA_KEYS.contains(key) =>
            case (key, value)                                   => fieldBuilder.prop(key, value)
          }

          fieldAvroType.getType match {
            case ENUM => fieldBuilder.`type`(fieldAvroType).noDefault()
            case _ =>
              getDefaultValue(f.metadata, DEFAULT) match {
                case Some(defaultObject)
                    if !Objects.equals(defaultObject, JsonProperties.NULL_VALUE) =>
                  fieldBuilder.`type`(fieldAvroType).withDefault(defaultObject)
                case Some(_) =>
                  fieldBuilder.`type`(fieldAvroType).withDefault(null)
                case _ => fieldBuilder.`type`(fieldAvroType).noDefault()
              }
          }
        }
        fieldsAssembler.endRecord()

      case _ => SchemaConverters.toAvroType(catalystType, nullable = false, recordName, namespace)
    }
    if (nullable && isNullable(metadata)) {
      getDefaultValue(metadata, DEFAULT) match {
        case Some(value) if !value.isInstanceOf[JsonProperties.Null] =>
          Schema.createUnion(schema, nullSchema)
        case _ => Schema.createUnion(nullSchema, schema)
      }
    } else {
      schema
    }
  }
}

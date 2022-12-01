package confluent.converter.avro

import confluent.converter.avro.implicits._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._
import scala.util.Try

object AvroToSparkSchemaConverter {

  private class IncompatibleSchemaException(msg: String, ex: Throwable = null)
      extends Exception(msg, ex)

  case class SchemaType(dataType: DataType, nullable: Boolean, avroType: Option[Schema])

  def toSqlType(avroSchema: Schema): SchemaType = {

    toSqlTypeHelper(avroSchema, Set.empty)
  }

  private def toSqlTypeHelper(avroSchema: Schema, existingRecordNames: Set[String]): SchemaType = {

    avroSchema.getType match {
      case RECORD =>
        if (existingRecordNames.contains(avroSchema.getFullName)) {
          throw new IncompatibleSchemaException(s"""
               |Found recursive reference in Avro schema, which can not be processed by Spark:
               |${avroSchema.toString(true)}
          """.stripMargin)
        }
        val newRecordNames = existingRecordNames + avroSchema.getFullName
        val fields = avroSchema.getFields.asScala.map { f =>

          val schemaType = toSqlTypeHelper(f.schema(), newRecordNames)
          val isNullable =
            Try(f.schema().getTypes.asScala.exists(_.getType == NULL)).toOption.getOrElse(false)

          val metadata = AvroMetadataBuilder()
            .putDefaults(f)
            .putDoc(f)
            .putExtraProps(f)

          schemaType.avroType match {
            case Some(avroType) =>
              StructField(
                f.name,
                schemaType.dataType,
                schemaType.nullable,
                metadata
                  .putSchemaType(avroType)
                  .putNullability(isNullable)
                  .build
              )

            case None =>
              StructField(
                f.name,
                schemaType.dataType,
                schemaType.nullable,
                metadata.putNullability(isNullable).build
              )
          }
        }.toArray

        SchemaType(StructType(fields), nullable = false, None)

      case ARRAY =>
        val schemaType = toSqlTypeHelper(avroSchema.getElementType, existingRecordNames)
        SchemaType(
          ArrayType(schemaType.dataType, containsNull = schemaType.nullable),
          nullable = false,
          schemaType.avroType
        )

      case MAP =>
        val schemaType = toSqlTypeHelper(avroSchema.getValueType, existingRecordNames)
        SchemaType(
          MapType(StringType, schemaType.dataType, valueContainsNull = schemaType.nullable),
          nullable = false,
          schemaType.avroType
        )

      case UNION =>
        if (avroSchema.getTypes.asScala.exists(_.getType == NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = avroSchema.getTypes.asScala.filterNot(_.getType == NULL)
          if (remainingUnionTypes.size == 1) {
            toSqlTypeHelper(remainingUnionTypes.head, existingRecordNames).copy(nullable = true)
          } else {
            toSqlTypeHelper(Schema.createUnion(remainingUnionTypes.asJava), existingRecordNames)
              .copy(nullable = true)
          }
        } else {
          avroSchema.getTypes.asScala.map(_.getType).toSeq match {
            case Seq(t1) =>
              toSqlTypeHelper(avroSchema.getTypes.get(0), existingRecordNames)
            case Seq(t1, t2) if Set(t1, t2) == Set(INT, LONG) =>
              SchemaType(LongType, nullable = false, Option(avroSchema))
            case Seq(t1, t2) if Set(t1, t2) == Set(FLOAT, DOUBLE) =>
              SchemaType(DoubleType, nullable = false, Option(avroSchema))
            case _ =>
              // Convert complex unions to struct types where field names are member0, member1, etc.
              // This is consistent with the behavior when converting between Avro and Parquet.
              val fields = avroSchema.getTypes.asScala.zipWithIndex.map { case (s, i) =>
                val schemaType = toSqlTypeHelper(s, existingRecordNames)
                // All fields are nullable because only one of them is set at a time
                StructField(s"member$i", schemaType.dataType, nullable = true)
              }

              SchemaType(StructType(fields.toSeq), nullable = false, None)
          }
        }
      case ENUM =>
        SchemaType(StringType, nullable = false, Option(avroSchema))
      case _ =>
        val originalSchemaType = SchemaConverters.toSqlType(avroSchema)
        SchemaType(originalSchemaType.dataType, originalSchemaType.nullable, Option(avroSchema))
    }
  }
}

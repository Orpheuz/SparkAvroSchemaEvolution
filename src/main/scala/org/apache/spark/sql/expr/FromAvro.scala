package org.apache.spark.sql.expr

import confluent.converter.avro.AvroToSparkSchemaConverter
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.spark.sql.Column
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, BinaryType, DataType}

import java.nio.ByteBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

case class FromAvro(
  child: Expression,
  schemaRegistryClientLambda: () => CachedSchemaRegistryClient,
  subject: String,
  options: Map[String, String]
) extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)

  private lazy val avroOptions = AvroOptions(options)

  @transient
  private lazy val schemaRegistryClient: CachedSchemaRegistryClient =
    schemaRegistryClientLambda()

  @transient
  private lazy val subjectIndex: mutable.SortedMap[Int, Schema] = {
    mutable.SortedMap(
      schemaRegistryClient.getAllVersions(subject).asScala.map {
        version =>
          val schemaMetadata = schemaRegistryClient.getSchemaMetadata(subject, version)
          val schema = new Schema.Parser().parse(schemaMetadata.getSchema)
          schemaMetadata.getId -> schema
      }.toSeq: _*
    )
  }

  @transient
  private lazy val latestSchema = subjectIndex.last._2

  override val nullable: Boolean = true

  override lazy val dataType: DataType = AvroToSparkSchemaConverter.toSqlType(latestSchema).dataType

  @transient
  private var decoder: BinaryDecoder = _

  @transient
  private var result: Any = _

  private def getIdFromMessage(message: Array[Byte]): Int = {

    val buffer = ByteBuffer.wrap(message.take(5))
    if(buffer.get() != 0) {
      throw new Exception("Unknown magic byte!")
    } else {
      buffer.getInt()
    }
  }

  override def nullSafeEval(input: Any): Any = {

    val binary = input.asInstanceOf[Array[Byte]]
    Try {
      val id = getIdFromMessage(binary)

      decoder = DecoderFactory.get().binaryDecoder(binary, 5, binary.length, decoder)

      result = new GenericDatumReader[Any](subjectIndex(id), latestSchema).read(null, decoder)

      val deserializer = new AvroDeserializer(latestSchema, dataType, avroOptions.datetimeRebaseModeInRead)
      val deserialized = deserializer.deserialize(result)
      assert(
        deserialized.isDefined,
        "Avro deserializer cannot return an empty result because filters are not pushed down")
      deserialized.get
    } match {
      // Should be handled better
      case Failure(e) => throw e
      case Success(value) => value
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {

    this.copy(child = newChild)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(
      ctx, ev, eval => {
        val result = ctx.freshName("result")
        val dt = CodeGenerator.boxedType(dataType)
        s"""
      $dt $result = ($dt) $expr.nullSafeEval($eval);
      if ($result == null) {
        ${ev.isNull} = true;
      } else {
        ${ev.value} = $result;
      }
    """
      })
  }
}

object FromAvro {

  def apply(column: Column, subject: String, registryURL: String, registryOptions: Map[String, _] = Map.empty, avroOptions: Map[String, String] = Map.empty): Column = {

    val clientLambda = () => {
      new CachedSchemaRegistryClient(
        registryURL,
        10,
        registryOptions.asJava
      )
    }

    new Column(
      FromAvro(
        child = column.expr,
        schemaRegistryClientLambda = clientLambda,
        subject = subject,
        options = avroOptions
      )
    )
  }
}
import confluent.converter.avro.SparkToAvroSchemaConverter
import kafka.{DemoKafkaAdmin, DemoKafkaProducer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expr.FromAvro
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SchemaEvolution {

  def main(args: Array[String]): Unit = {

    val bootstrapServers = args(0)
    val topicName = args(1)
    val schemaRegistryURL = args(2)
    val schemaRegistryUserInfo = args(3)
    val schemaRegistryOptions = Map(
      "basic.auth.credentials.source" -> "USER_INFO",
      "basic.auth.user.info" -> schemaRegistryUserInfo
    )

    val kafkaAdmin = new DemoKafkaAdmin(bootstrapServers, topicName)
    kafkaAdmin.createTopic()

    (1 to 3).foreach(version => run(
      version, bootstrapServers, topicName, schemaRegistryURL, schemaRegistryOptions)
    )
  }

  def run(
    version: Int,
    bootstrapServers: String,
    topicName: String,
    schemaRegistryURL: String,
    schemaRegistryOptions: Map[String, String]
  ): Unit = {

    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName("AvroSchemaEvolutionDemo")
      .getOrCreate()

    val producer = new DemoKafkaProducer(bootstrapServers, topicName, schemaRegistryURL, schemaRegistryOptions)

    producer.produceNewVersion(version)

    val df = ss
      .readStream
      .format("kafka")
      .option("startingOffsets", "earliest")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicName)
      .load()

    val transformedDF = df
      .withColumn(
        "value",
        FromAvro(
          col("value"),
          s"$topicName-value",
          schemaRegistryURL,
          schemaRegistryOptions
        )
      ).select("value.*")

    val streamingQuery = transformedDF
      .writeStream
      .format("avro")
      .trigger(Trigger.Once())
      .option(
        "avroSchema",
        SparkToAvroSchemaConverter
          .toAvroType(
            catalystType = transformedDF.schema,
            namespace = "com.example.avro",
            recordName = "Record"
          ).toString)
      .option("checkpointLocation", "/tmp/demo-checkpoint")
      .option("path", "results")
      .outputMode(OutputMode.Append())
      .start()

    streamingQuery.awaitTermination()
  }
}
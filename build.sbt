ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "SchemaEvolution"
  )

resolvers ++= Seq(
  "Confluent" at "https://packages.confluent.io/maven/",
)

val schemaRegistryVersion = "5.3.0"
val sparkVersion = "3.3.1"

libraryDependencies += "io.confluent" % "kafka-schema-registry-client" % schemaRegistryVersion
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % schemaRegistryVersion
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-avro" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.avro" % "avro" % "1.11.1"
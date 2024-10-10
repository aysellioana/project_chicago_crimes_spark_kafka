ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "project_crimes_spark_kafka",
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-sql" % "3.2.0" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-mllib" % "3.2.0" withSources() withJavadoc(),
  //"org.apache.spark" %% "spark-sql-kafka-0-10_2.12" % "3.5.3",
  // Removed "spark-streaming-kafka-0-10" and added "spark-sql-kafka-0-10"
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0", // For Kafka integration with Structured Streaming
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
  "org.apache.kafka" %% "kafka" % "3.0.0",
  "com.typesafe" % "config" % "1.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.0",
  "org.postgresql" % "postgresql" % "42.2.18",
  "io.circe" %% "circe-core" % "0.14.1",
  "io.circe" %% "circe-generic" % "0.14.1",
  "io.circe" %% "circe-parser" % "0.14.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.0",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % "2.13.3"
)

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Load configuration
    val config = ConfigFactory.load()
    val kafkaBrokers = config.getString("kafka.brokers")
    val kafkaTopic = config.getString("kafka.topic")

    // Create Spark session
    val spark = SparkSession.builder
      .appName("Crime Kafka Consumer")
      .master("local[*]")
      .getOrCreate()



    import spark.implicits._

    // Read data from Kafka
    val dfChicago = spark.read
      .parquet("/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/percentage_crimes")

    dfChicago.show()

  }
}
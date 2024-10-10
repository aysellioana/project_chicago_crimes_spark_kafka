import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object CrimeProducerMain {
  def main(args: Array[String]): Unit = {
    // Set the logging level to ERROR to minimize console output
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Load application configuration from the config file
    val config = ConfigFactory.load()
    val apiUrl = config.getString("api.url")
    val appToken = config.getString("api.app-token")
    val kafkaBrokers = config.getString("kafka.brokers")
    val kafkaTopic = config.getString("kafka.topic")

    implicit val spark: SparkSession = SparkSession.builder
      .appName("CrimeProducer")
      .master("local[*]")
      .getOrCreate()

    // Initialize ObjectMapper for JSON serialization/deserialization
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    val crimeApiService = new CrimeApiService(apiUrl, appToken, objectMapper)

    val producer = new CrimeProducer(kafkaBrokers, kafkaTopic)

    var offset = 0
    val limit = 1000

    while (true) {
      val (statusCode, crimeRecords) = crimeApiService.fetchCrimeData(limit, offset)

      if (statusCode == 200 && crimeRecords.nonEmpty) {
        // For each record in the list of crimes, serialize it to JSON and send to Kafka
        crimeRecords.foreach { crime =>
          // Serialize the Crime object to JSON
          val jsonString = objectMapper.writeValueAsString(crime)

          // Send the JSON string to Kafka
          producer.sendCrime(jsonString)
          println(s"Sent crime record to Kafka: $jsonString")
        }

        offset += limit
      } else {
        println(s"No more data available or failed to fetch: $statusCode")
        return
      }
    }

    producer.close()
    spark.stop()
  }
}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object CrimeConsumerMain {
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

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


    import spark.implicits._

    // Read data from Kafka
    val dfChicago = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "50000")
      .load()


    // Convert Kafka value to string
    val dfRaw = dfChicago.selectExpr("CAST(value AS STRING) AS json")

    // Define the schema from the Crime case class
    val schema = Seq[Crime]().toDF.schema

    // Parse the JSON data into a structured DataFrame
    val dfStructured = dfRaw.select(from_json($"json", schema).as("data")).select("data.*")

    dfStructured.printSchema()

    //    // Output the structured DataFrame to the console for debugging
    //    val query = dfStructured.writeStream
    //      .format("console")
    //      .outputMode("append")
    //      .start()

    //Percentage of domestic crimes
    val dfWithTimestamp_1 = dfStructured
      .withColumn("timestamp", to_timestamp($"date", "yyyy-MM-dd'T'HH:mm:ss"))
      .withWatermark("timestamp", "10 minutes")

    val percentage_crimes = dfWithTimestamp_1
      .groupBy(window($"timestamp", "10 minutes"))
      .agg(
        count("*").alias("total_crimes"),
        count(when($"domestic", true)).alias("domestic_crimes")
      )
      .select(
        $"total_crimes",
        $"domestic_crimes",
        round($"domestic_crimes" / $"total_crimes" * 100, 2).alias("percentage_domestic_crimes")
      )
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/percentage_crimes")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/percentage_crimes")
      .outputMode("append")
      .start()

    //    //The number of crimes per year
    val crimes_per_year = dfWithTimestamp_1
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window($"timestamp", "1 day")) // Use a smaller window (1 day)
      .agg(
        count("*").alias("total_crimes")
      )
      .select(year($"window.start").alias("year"), $"total_crimes") // Extract the year from the window
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/crimes_per_year")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/crimes_per_year")
      .outputMode("append") // Use append mode
      .start()

    //    //Where do most crimes take place
    val most_crimes = dfWithTimestamp_1
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window($"timestamp", "1 hour"), $"location_description") // Group by a 1-hour window and location
      .agg(
        count("*").alias("most_crimes") // Count crimes per location
      )
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/most_crimes")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/most_crimes")
      .outputMode("append")
      .start()

    //    //Which days have the highest number of crimes
    val dfWithDate = dfStructured
      .withColumn("timestamp", to_timestamp($"date", "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("hour", hour($"timestamp"))
      .withColumn("day", date_format($"timestamp", "EEEE"))

    val days_highest_no_crimes = dfWithDate
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window($"timestamp", "1 day"), $"day")
      .agg(
        count("*").alias("highest_no_crimes")
      )
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/days_highest_no_crimes")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/days_highest_no_crimes")
      .outputMode("append") // Use append mode
      .start()


    val dfWithDate2 = dfStructured
      .withColumn("timestamp", to_timestamp($"date", "yyyy-MM-dd'T'HH:mm:ss"))
      .withWatermark("timestamp", "10 minutes")
      .withColumn("hour", hour($"timestamp"))
      .withColumn("day", date_format($"timestamp", "EEEE"))

    val crimes_by_day_windowed = dfWithDate2
      .groupBy(
        window($"timestamp", "1 day"),
        $"day"
      )
      .agg(
        count("*").alias("highest_no_crimes")
      )
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/crimes_by_day_windowed")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/crimes_by_day_windowed")
      .start()

    //    WINDOW::: =>Create a time window of 1 day. Instead of grouping all events into a single day of the week (eg all months),
    //    it will group all crimes that happened within a one-day time window (regardless of the day of the week).
    // !!!~ dates will be grouped based on each full calendar day


    //Number of domestic crimes by hour write to kafka
    //    val domestic_crimes_by_hour = dfWithDate2
    //      .groupBy(
    //        window($"timestamp", "1 hour"),
    //        $"hour"
    //      )
    //      .agg(
    //        count(when($"domestic", true)).alias("domestic_crimes")
    //      )
    //      .withColumn("value", to_json(struct(
    //        $"window.start".alias("date"),
    //        $"hour",
    //        $"domestic_crimes"
    //      )))
    //      .writeStream
    //      .format("kafka")
    //      .option("kafka.bootstrap.servers", kafkaBrokers)
    //      .option("topic", "domestic_crimes_by_hour")
    //      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint")
    //      .outputMode("append")
    //      .start()


    //Number of domestic crimes by hour write to kafka
    val domestic_crimes_by_hour = dfWithDate2
      .groupBy(
        window($"timestamp", "1 hour"),
        $"hour"
      )
      .agg(
        count(when($"domestic", true)).alias("domestic_crimes")
      )
      .withColumn("value", to_json(struct(
        $"window.start".alias("date"),
        $"hour",
        $"domestic_crimes"
      )))
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/domestic_crimes_by_hour")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/domestic_crimes_by_hour")
      .start()

    //Number of domestic crimes by hour/ day
    val domestic_crimes = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "domestic_crimes_by_hour")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", "510")
      .load()

    val dfRaw2 = domestic_crimes.selectExpr("CAST(value AS STRING) AS json")
    val schemaDomestic = StructType(Array(
      StructField("date", StringType, nullable = true),
      StructField("hour", StringType, nullable = true),
      StructField("domestic_crimes", StringType, nullable = true)
    ))
    val dfStructured2 = dfRaw2.select(from_json($"json", schemaDomestic).as("date")).select("date.*")


    val dfWithTimestamp = dfStructured2
      .withColumn("timestamp", to_timestamp($"date", "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("day", date_format($"timestamp", "EEEE"))

    val domestic_crimes_by_day = dfWithTimestamp
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "1 day"),
        $"day"
      )
      .agg(
        count($"domestic_crimes").alias("total_domestic_crimes")
      )

      domestic_crimes_by_day
      .writeStream
      .format("parquet")
      .option("path", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/output_parquet/domestic_crimes_by_day")
      .option("checkpointLocation", "/Users/abatcoveanu/Documents/Projects/project_crimes_spark_kafka/checkpoint/domestic_crimes_by_day")
      .start()
        .awaitTermination()

  }
}

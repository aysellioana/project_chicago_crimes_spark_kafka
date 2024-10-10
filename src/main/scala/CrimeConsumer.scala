import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Properties, UUID}
import scala.collection.JavaConverters._

class CrimeConsumer(brokers: String, topic: String) {

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put("max.poll.records", 1000)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()) // Use a unique group ID for each run to read all messages.
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Read from the earliest if no offsets exist.
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true") // Automatically commit offsets.

  val consumer = new KafkaConsumer[String, String](props)

  def consumeMessages(): Unit = {
    consumer.subscribe(java.util.Collections.singletonList(topic))

    println(s"Listening for messages on topic: $topic")

    try {
      var isActive = true
      var cont = 10
      while (isActive) {
        cont = cont -1
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        println(s" ${records.count()} messages.")

        if (records.isEmpty && cont < 0) {
          isActive = false;
          println("No messages received.")
        } else {
          for (record <- records.asScala) {
            println(s"Received: ${record.value()} la offset ${record.offset()}")

          }
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}



//val consumer = new CrimeConsumer(kafkaBrokers, kafkaTopic)
//consumer.consumeMessages()

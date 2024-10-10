import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties

class CrimeProducer(brokers: String, topic: String) {

  // Kafka producer configuration
  private val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")

  private val producer = new KafkaProducer[String, String](props)

  // Method for sending a single JSON string to Kafka
  def sendCrime(jsonString: String): Unit = {
    val producerRecord = new ProducerRecord[String, String](topic, jsonString)
    producer.send(producerRecord)
    println(s"Sent record to Kafka topic $topic: $jsonString")
  }

  // Close the producer
  def close(): Unit = {
    producer.close()
  }
}

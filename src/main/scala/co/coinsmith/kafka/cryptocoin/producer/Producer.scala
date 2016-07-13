package co.coinsmith.kafka.cryptocoin.producer

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Producer {
  val conf = ConfigFactory.load
  val brokers = conf.getString("kafka.cryptocoin.bootstrap-servers")

  val props = new Properties
  props.put("bootstrap.servers", brokers)
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("request.required.acks", "1")
  val producer = new KafkaProducer[String, String](props)

  def send(topic: String, msg: String) {
    val data = new ProducerRecord[String, String](topic, msg)
    producer.send(data)
  }
}

package co.coinsmith.kafka.cryptocoin

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

object KafkaProducer {
  val conf = ConfigFactory.load
  val brokers = conf.getString("kafka.cryptocoin.broker-list")

  val props = new Properties
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  val producerConfig = new ProducerConfig(props)
  val producer = new Producer[String, String](producerConfig)

  def send(topic: String, key: String, msg: String) {
    val data = new KeyedMessage[String, String](topic, key, msg)
    producer.send(data)
  }
}

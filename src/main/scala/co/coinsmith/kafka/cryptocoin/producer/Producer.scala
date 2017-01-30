package co.coinsmith.kafka.cryptocoin.producer

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object Producer {
  val conf = ConfigFactory.load
  val brokers = conf.getString("kafka.cryptocoin.bootstrap-servers")
  val schemaRegistryUrl = conf.getString("kafka.cryptocoin.schema-registry-url")

  val props = new Properties
  props.put("bootstrap.servers", brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put("acks", "all")
  props.put("schema.registry.url", schemaRegistryUrl)
  val producer = new KafkaProducer[Object, Object](props)

  def send(topic: String, key: Object, value: Object) = {
    val data = new ProducerRecord[Object, Object](topic, key, value)
    producer.send(data)
  }

  def send(topic: String, value: Object) {
    val data = new ProducerRecord[Object, Object](topic, value)
    producer.send(data)
  }
}

package co.coinsmith.kafka.cryptocoin.producer

import java.io.FileInputStream
import java.util.{Properties, UUID}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

object Producer {
  val logger = LoggerFactory.getLogger(this.getClass)

  val uuid = UUID.randomUUID
  logger.info("Producer UUID: {}", uuid)

  val conf = ConfigFactory.load
  val brokers = conf.getString("kafka.cryptocoin.bootstrap-servers")
  val schemaRegistryUrl = conf.getString("kafka.cryptocoin.schema-registry-url")

  val props = new Properties
  props.put("acks", "all")

  val producerConfigPath = "kafka.cryptocoin.producer.config"
  if (conf.hasPath(producerConfigPath)) {
    val filename = conf.getString(producerConfigPath)
    val propsFile = new FileInputStream(filename)
    props.load(propsFile)
  }

  props.put("bootstrap.servers", brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
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

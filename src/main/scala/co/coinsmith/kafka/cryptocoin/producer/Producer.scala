package co.coinsmith.kafka.cryptocoin.producer

import java.io.FileInputStream
import java.util.{Properties, UUID}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer._
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
  props.put("retries", Integer.MAX_VALUE.toString)
  props.put("max.in.flight.requests.per.connection", "1")

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

  def callbackWithData(data: ProducerRecord[Object, Object]): Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) {
        exception.printStackTrace()
        producer.send(data, callbackWithData(data))
      }
    }
  }

  def send(topic: String, key: Object, value: Object) = {
    val data = new ProducerRecord[Object, Object](topic, key, value)
    producer.send(data, callbackWithData(data))
  }

  def send(topic: String, value: Object) {
    val data = new ProducerRecord[Object, Object](topic, value)
    producer.send(data, callbackWithData(data))
  }
}

package co.coinsmith.kafka.cryptocoin.producer

import scala.util.{Failure, Success, Try}
import java.io.FileInputStream
import java.util.{Properties, UUID}

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Source}
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

object KafkaCryptocoinProducer {
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

  def getProducerSink(implicit system: ActorSystem) = {
    val settings = ProducerSettings(system, new KafkaAvroSerializer, new KafkaAvroSerializer)
    Producer.plainSink(settings, producer)
  }

  def producerComplete(td: Try[Done]) = td match {
    case Success(d) =>
    case Failure(ex) => ex.printStackTrace()
  }

  def send(topic: String, key: Object, value: Object)(implicit system: ActorSystem, materializor: ActorMaterializer) = {
    import system.dispatcher
    val data = new ProducerRecord[Object, Object](topic, key, value)
    val closed = Source.single(data).runWith(getProducerSink)
    closed.onComplete(producerComplete)
  }

  def send(topic: String, value: Object)(implicit system: ActorSystem, materializor: ActorMaterializer) {
    import system.dispatcher
    val data = new ProducerRecord[Object, Object](topic, value)
    val closed = Source.single(data).runWith(getProducerSink)
    closed.onComplete(producerComplete)
  }
}

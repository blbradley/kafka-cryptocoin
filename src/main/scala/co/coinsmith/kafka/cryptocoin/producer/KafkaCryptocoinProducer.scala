package co.coinsmith.kafka.cryptocoin.producer

import scala.util.{Failure, Success, Try}
import java.io.{ByteArrayOutputStream, FileInputStream, ObjectOutputStream}
import java.nio.file.Paths
import java.util.{Properties, UUID}

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{FileIO, Flow, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.KafkaCryptocoin
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.Serializer

object KafkaCryptocoinProducer {
  import KafkaCryptocoin.system
  implicit val materializer = ActorMaterializer()
  val logger = Logging(system.eventStream, this.getClass.getName)

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

  val keySerializerField = producer.getClass.getDeclaredField("keySerializer")
  keySerializerField.setAccessible(true)
  val keySerializer = keySerializerField.get(producer).asInstanceOf[Serializer[Object]]
  println(keySerializer)

  val valueSerializerField = producer.getClass.getDeclaredField("valueSerializer")
  valueSerializerField.setAccessible(true)
  val valueSerializer = valueSerializerField.get(producer).asInstanceOf[Serializer[Object]]
  println(valueSerializer)

  val queueSize = 1000
  val queueSource = Source.queue[ProducerRecord[Object, Object]](queueSize, OverflowStrategy.backpressure)
  val recordByteFlow = Flow[ProducerRecord[Object, Object]].map { record =>
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    val serializedKey = keySerializer.serialize(record.topic, record.key)
    val serializedValue =valueSerializer.serialize(record.topic, record.value)
    objectStream.writeObject((record.topic, serializedKey, serializedValue))
    objectStream.close
    ByteString(byteStream.toByteArray)
  }
  val fileSink = FileIO.toPath(Paths.get("/tmp/kafka-cryptocoin-log"))
  val queue = queueSource.via(recordByteFlow).to(fileSink).run

  def getProducerSink(implicit system: ActorSystem) = {
    val settings = ProducerSettings(system, new KafkaAvroSerializer, new KafkaAvroSerializer)
    Producer.plainSink(settings, producer)
  }

  def producerComplete(record: ProducerRecord[Object, Object])(td: Try[Done]) = td match {
    case Success(d) =>
    case Failure(ex) => queue.offer(record)
  }

  def send(topic: String, key: Object, value: Object)(implicit system: ActorSystem, materializer: ActorMaterializer) = {
    import system.dispatcher
    val data = new ProducerRecord[Object, Object](topic, key, value)
    val closed = Source.single(data).runWith(getProducerSink)
    closed.onComplete(producerComplete(data))
  }

  def send(topic: String, value: Object)(implicit system: ActorSystem, materializer: ActorMaterializer): Unit =
    send(topic, null, value)
}

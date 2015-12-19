package co.coinsmith.kafka.cryptocoin

import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}


object KafkaCryptocoin {
  val corePoolSize = ExchangeService.getExchanges.size
  val scheduler = Executors.newScheduledThreadPool(corePoolSize)
  val brokers = sys.env("KAFKA_CRYPTOCOIN_BROKER_LIST")

  val props = new Properties
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  val producerConfig = new ProducerConfig(props)

  def main(args: Array[String]) {
    val producer = new Producer[String, String](producerConfig)

    ExchangeService.getExchanges foreach { exchange =>
      try {
        scheduler.scheduleAtFixedRate(new TickProducer(exchange, producer), 0, 30, SECONDS)
      } catch {
        case e: Exception => {
          producer.close
          throw new RuntimeException
        }
      }
    }

  }
}

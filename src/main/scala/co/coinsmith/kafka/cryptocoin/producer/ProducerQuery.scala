package co.coinsmith.kafka.cryptocoin.producer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import co.coinsmith.kafka.cryptocoin.{ExchangeEvent, ProducerKey}
import co.coinsmith.kafka.cryptocoin.streaming.WebsocketMessage
import org.apache.kafka.clients.producer.ProducerRecord


class ProducerQuery(persistenceId: String)(implicit system: ActorSystem) {
  implicit val mat = ActorMaterializer()
  val queries = PersistenceQuery(system).readJournalFor[LeveldbReadJournal](
    LeveldbReadJournal.Identifier)

  val src: Source[EventEnvelope, NotUsed] =
    queries.eventsByPersistenceId(persistenceId, 0L, Long.MaxValue)

  val events: Source[Any, NotUsed] = src.map(_.event)
  val producerFlow = Producer.flow[Object, Object, WebsocketMessage](KafkaCryptocoinProducer.settings, KafkaCryptocoinProducer.producer)

//  events.runForeach(e => println(e))
  events.map {
    case wsm @ WebsocketMessage(timestamp, exchange, msg) =>
      val key = ProducerKey(KafkaCryptocoinProducer.uuid, exchange)
      val event = ExchangeEvent(timestamp, KafkaCryptocoinProducer.uuid, exchange, msg)
      val record = new ProducerRecord[Object, Object]("streaming.websocket.raw", ProducerKey.format.to(key), ExchangeEvent.format.to(event))
      ProducerMessage.Message(record, wsm)
  } via(producerFlow) runWith Sink.ignore
}

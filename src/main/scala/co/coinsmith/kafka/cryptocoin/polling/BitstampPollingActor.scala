package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import org.json4s.jackson.JsonMethods._

case class BitstampPollingTick(
  high: String,
  last: String,
  timestamp: String,
  bid: String,
  vwap: String,
  volume: String,
  low: String,
  ask: String,
  open: Double
)
object BitstampPollingTick {
  implicit def toTick(tick: BitstampPollingTick) =
    Tick(tick.last.toDouble, tick.bid.toDouble, tick.ask.toDouble, Some(tick.high.toDouble), Some(tick.low.toDouble), Some(tick.open), volume = Some(tick.volume.toDouble), vwap = Some(tick.vwap.toDouble), timestamp = Some(Instant.ofEpochSecond(tick.timestamp.toLong)))
}

case class BitstampPollingOrderBook(
  timestamp: String,
  bids: List[List[String]],
  asks: List[List[String]]
)
object BitstampPollingOrderBook {
  val toOrder = { o: List[String] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: BitstampPollingOrderBook) =
    OrderBook(
      ob.bids map toOrder,
      ob.asks map toOrder,
      Some(Instant.ofEpochSecond(ob.timestamp.toLong))
    )
}

class BitstampPollingActor extends HTTPPollingActor with ProducerBehavior {
  val topicPrefix = "bitstamp.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.bitstamp.net")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val msg = parse(responseEntityToString(entity))
    val tick = msg.extract[BitstampPollingTick]

    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val msg = parse(responseEntityToString(entity))
    val ob = msg.extract[BitstampPollingOrderBook]

    ("orderbook", OrderBook.format.to(ob))
  }

  def receive = periodicBehavior orElse producerBehavior orElse {
    case "tick" =>
      request("/api/ticker/")
        .via(tickFlow)
        .runWith(selfSink)
    case "orderbook" =>
      request("/api/order_book/")
        .via(orderbookFlow)
        .runWith(selfSink)
  }
}

package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.producer.Producer
import co.coinsmith.kafka.cryptocoin.{ExchangeEvent, Order, OrderBook, Tick}

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
  implicit def toTick(tick: BitstampPollingTick)(implicit timeCollected: Instant) =
    Tick(
      tick.last.toDouble, tick.bid.toDouble, tick.ask.toDouble, timeCollected,
      Some(tick.high.toDouble), Some(tick.low.toDouble), Some(tick.open),
      volume = Some(tick.volume.toDouble), vwap = Some(tick.vwap.toDouble),
      timestamp = Some(Instant.ofEpochSecond(tick.timestamp.toLong))
    )
}

case class BitstampPollingOrderBook(
  timestamp: String,
  bids: List[List[String]],
  asks: List[List[String]]
)
object BitstampPollingOrderBook {
  val toOrder = { o: List[String] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: BitstampPollingOrderBook)(implicit timeCollected: Instant) =
    OrderBook(
      ob.bids map toOrder,
      ob.asks map toOrder,
      Some(Instant.ofEpochSecond(ob.timestamp.toLong)),
      Some(timeCollected)
    )
}

class BitstampPollingActor extends HTTPPollingActor {
  import akka.pattern.pipe
  import context.dispatcher

  val exchange = "bitstamp"
  val topicPrefix = "bitstamp.polling.btcusd."
  val http = Http(context.system)

  val tickFlow = Flow[(Instant, BitstampPollingTick)].map { case (t, tick) =>
    implicit val timeCollected = t
    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, BitstampPollingOrderBook)].map { case (t, ob) =>
    implicit val timeCollected = t
    ("orderbook", OrderBook.format.to(ob))
  }

  def receive = periodicBehavior orElse responseBehavior orElse {
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
    case "tick" =>
      http.singleRequest(HttpRequest(uri = "https://www.bitstamp.net/api/v2/ticker/btcusd/")) pipeTo self
    case "orderbook" =>
      http.singleRequest(HttpRequest(uri = "https://www.bitstamp.net/api/v2/order_book/btcusd/")) pipeTo self
  }
}

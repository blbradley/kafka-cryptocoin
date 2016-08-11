package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick, Utils}
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
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
    Tick(
      BigDecimal(tick.last),
      BigDecimal(tick.bid),
      BigDecimal(tick.ask),
      BigDecimal(tick.high),
      BigDecimal(tick.low),
      BigDecimal(tick.vwap),
      BigDecimal(tick.volume),
      BigDecimal(tick.open),
      Instant.ofEpochSecond(tick.timestamp.toLong).toString
    )
}

case class BitstampPollingOrderBook(
  timestamp: String,
  bids: List[List[String]],
  asks: List[List[String]]
)
object BitstampPollingOrderBook {
  implicit def toOrderBook(ob: BitstampPollingOrderBook) = {
    val bids = ob.bids map { o => Order(BigDecimal(o(0)), BigDecimal(o(1)))}
    val asks = ob.asks map { o => Order(BigDecimal(o(0)), BigDecimal(o(1)))}
    OrderBook(bids, asks, Some(Instant.ofEpochSecond(ob.timestamp.toLong)))
  }
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

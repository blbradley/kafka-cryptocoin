package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.stream.scaladsl.{Flow, Sink}
import co.coinsmith.kafka.cryptocoin.producer.Producer
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}


case class BitfinexPollingTick(mid: String, bid: String, ask: String, last_price: String, timestamp: String)
object BitfinexPollingTick {
  implicit def toTick(tick: BitfinexPollingTick)(implicit timeCollected: Instant) = {
    val Array(seconds, nanos) = tick.timestamp.split('.').map { _.toLong }
    Tick(
      tick.last_price.toDouble, tick.bid.toDouble, tick.ask.toDouble, timeCollected,
      timestamp = Some(Instant.ofEpochSecond(seconds, nanos))
    )
  }
}

case class BitfinexPollingOrder(price: String, amount: String, timestamp: String)

case class BitfinexPollingOrderBook(bids: List[BitfinexPollingOrder], asks: List[BitfinexPollingOrder])
object BitfinexPollingOrderBook {
  def toOrder(o: BitfinexPollingOrder) =
    Order(BigDecimal(o.price), BigDecimal(o.amount), timestamp = Some(Instant.ofEpochSecond(o.timestamp.toDouble.toLong)))

  implicit def toOrderBook(ob: BitfinexPollingOrderBook)(implicit timeCollected: Instant) =
    OrderBook(
      ob.bids map toOrder,
      ob.asks map toOrder,
      timeCollected = Some(timeCollected)
    )
}

class BitfinexPollingActor extends HTTPPollingActor {
  val exchange = "bitfinex"
  val topicPrefix = "bitfinex.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("api.bitfinex.com")

  val tickFlow = Flow[(Instant, BitfinexPollingTick)].map { case (t, tick) =>
    implicit val timeCollected = t
    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, BitfinexPollingOrderBook)].map { case (t, ob) =>
    implicit val timeCollected = t
    ("orderbook", OrderBook.format.to(ob))
  }

  def receive = periodicBehavior orElse {
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
    case "tick" =>
      val req = request("/v1/pubticker/btcusd")
      if (preprocess) {
        req.via(convertFlow[BitfinexPollingTick])
          .via(tickFlow)
          .runWith(selfSink)
      } else {
        req.runWith(Sink.ignore)
      }
    case "orderbook" =>
      val req = request("/v1/book/btcusd")
      if (preprocess) {
        req.via(convertFlow[BitfinexPollingOrderBook])
          .via(orderbookFlow)
          .runWith(selfSink)
      } else {
        req.runWith(Sink.ignore)
      }
  }
}

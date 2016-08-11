package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._


case class BitfinexPollingTick(mid: String, bid: String, ask: String, last: String, timestamp: String)
object BitfinexPollingTick {
  implicit def toTick(tick: BitfinexPollingTick) = {
    val Array(seconds, nanos) = tick.timestamp.split('.').map { _.toLong }
    Tick(
      tick.last,
      tick.bid,
      tick.ask,
      Instant.ofEpochSecond(seconds, nanos)
    )
  }
}

case class BitfinexPollingOrder(price: String, amount: String, timestamp: String)

case class BitfinexPollingOrderBook(bids: List[BitfinexPollingOrder], asks: List[BitfinexPollingOrder])
object BitfinexPollingOrderBook {
  def toOrder(o: BitfinexPollingOrder) =
    Order(BigDecimal(o.price), BigDecimal(o.amount), Some(Instant.ofEpochSecond(o.timestamp.toDouble.toLong)))

  implicit def toOrderBook(ob: BitfinexPollingOrderBook) = OrderBook(ob.bids map toOrder, ob.asks map toOrder)
}

class BitfinexPollingActor extends HTTPPollingActor with ProducerBehavior {
  val topicPrefix = "bitfinex.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("api.bitfinex.com")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val msg = parse(responseEntityToString(entity)) transformField {
      case JField("last_price", v) => JField("last", v)
    }
    val tick = msg.extract[BitfinexPollingTick]

    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val msg = parse(responseEntityToString(entity))
    val ob = msg.extract[BitfinexPollingOrderBook]

    ("orderbook", OrderBook.format.to(ob))
  }

  def receive = periodicBehavior orElse producerBehavior orElse {
    case "tick" =>
      request("/v1/pubticker/btcusd")
        .via(tickFlow)
        .runWith(selfSink)
    case "orderbook" =>
      request("/v1/book/btcusd")
        .via(orderbookFlow)
        .runWith(selfSink)
  }
}

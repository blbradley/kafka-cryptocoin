package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ResponseEntity
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}
import co.coinsmith.kafka.cryptocoin.producer.ProducerBehavior
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._

case class OKCoinPollingTick(
  buy: String,
  high: String,
  last: String,
  low: String,
  sell: String,
  vol: String
)
case class OKCoinPollingDatedTick(date: String, ticker: OKCoinPollingTick)
object OKCoinPollingDatedTick  {
  implicit def toTick(datedTick: OKCoinPollingDatedTick) = {
    val tick = datedTick.ticker
    Tick(tick.last, tick.buy, tick.sell,
      tick.high, tick.low, None,
      tick.vol, None, Instant.ofEpochSecond(datedTick.date.toLong))
  }
}

case class OKCoinPollingOrderBook(asks: List[List[Double]], bids: List[List[Double]])
object OKCoinPollingOrderBook {
  val toOrder = { o: List[Double] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: OKCoinPollingOrderBook) =
    OrderBook(
      ob.bids map toOrder,
      ob.asks map toOrder,
      None
    )
}

class OKCoinPollingActor extends HTTPPollingActor with ProducerBehavior {
  val topicPrefix = "okcoin.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.okcoin.cn")

  val tickFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val msg = parse(responseEntityToString(entity))
    val tick = msg.extract[OKCoinPollingDatedTick]

    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, ResponseEntity)].map { case (t, entity) =>
    val msg = parse(responseEntityToString(entity))
    val ob = msg.extract[OKCoinPollingOrderBook]

    ("orderbook", OrderBook.format.to(ob))
  }

  def receive = periodicBehavior orElse producerBehavior orElse {
    case "tick" =>
      request("/api/v1/ticker.do?symbol=btc_cny")
        .via(tickFlow)
        .runWith(selfSink)
    case "orderbook" =>
      request("/api/v1/depth.do??symbol=btc_cny")
        .via(orderbookFlow)
        .runWith(selfSink)
  }
}

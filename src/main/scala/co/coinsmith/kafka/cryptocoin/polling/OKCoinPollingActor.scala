package co.coinsmith.kafka.cryptocoin.polling

import java.time.Instant

import akka.http.scaladsl.Http
import akka.stream.scaladsl.Flow
import co.coinsmith.kafka.cryptocoin.producer.Producer
import co.coinsmith.kafka.cryptocoin.{Order, OrderBook, Tick}

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
  implicit def toTick(datedTick: OKCoinPollingDatedTick)(implicit timeCollected: Instant) = {
    val tick = datedTick.ticker
    Tick(tick.last.toDouble, tick.buy.toDouble, tick.sell.toDouble, timeCollected,
      Some(tick.high.toDouble), Some(tick.low.toDouble), None,
      volume = Some(tick.vol.toDouble), vwap = None,
      timestamp = Some(Instant.ofEpochSecond(datedTick.date.toLong))
    )
  }
}

case class OKCoinPollingOrderBook(asks: List[List[Double]], bids: List[List[Double]])
object OKCoinPollingOrderBook {
  val toOrder = { o: List[Double] => Order(o(0), o(1)) }

  implicit def toOrderBook(ob: OKCoinPollingOrderBook)(implicit timeCollected: Instant) =
    OrderBook(
      ob.bids map toOrder,
      ob.asks map toOrder,
      None,
      Some(timeCollected)
    )
}

class OKCoinPollingActor extends HTTPPollingActor {
  val topicPrefix = "okcoin.polling.btcusd."
  val pool = Http(context.system).cachedHostConnectionPoolHttps[String]("www.okcoin.cn")

  val tickFlow = Flow[(Instant, OKCoinPollingDatedTick)].map { case (t, tick) =>
    implicit val timeCollected = t
    ("ticks", Tick.format.to(tick))
  }

  val orderbookFlow = Flow[(Instant, OKCoinPollingOrderBook)].map { case (t, ob) =>
    implicit val timeCollected = t
    ("orderbook", OrderBook.format.to(ob))
  }

  def receive = periodicBehavior orElse {
    case (topic: String, value: Object) =>
      Producer.send(topicPrefix + topic, value)
    case "tick" =>
      request("/api/v1/ticker.do?symbol=btc_cny")
        .via(convertFlow[OKCoinPollingDatedTick])
        .via(tickFlow)
        .runWith(selfSink)
    case "orderbook" =>
      request("/api/v1/depth.do??symbol=btc_cny")
        .via(convertFlow[OKCoinPollingOrderBook])
        .via(orderbookFlow)
        .runWith(selfSink)
  }
}

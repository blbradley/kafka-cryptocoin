package co.coinsmith.kafka.cryptocoin

import java.time.Instant

import co.coinsmith.kafka.cryptocoin.avro.InstantTypeMaps._
import com.sksamuel.avro4s.RecordFormat


case class Tick(last: BigDecimal, bid: BigDecimal, ask: BigDecimal,
                high: Option[BigDecimal] = None, low: Option[BigDecimal] = None, open: Option[BigDecimal] = None,
                volume: Option[BigDecimal] = None, vwap: Option[BigDecimal] = None,
                bidVolume: Option[BigDecimal] = None, askVolume: Option[BigDecimal] = None,
                lastDailyChange: Option[BigDecimal] = None, lastDailyChangePercent: Option[BigDecimal] = None,
                timestamp: Option[Instant] = None)
object Tick {
  val format = RecordFormat[Tick]
}

case class Order(price: BigDecimal, volume: BigDecimal, timestamp: Option[Instant] = None)
object Order {
  def apply(price: String, volume: String) = new Order(BigDecimal(price), BigDecimal(volume))
}

case class OrderBook(bids: List[Order], asks: List[Order], timestamp: Option[Instant] = None)
object OrderBook {
  val format = RecordFormat[OrderBook]
}

case class Trade(tid: Long, price: BigDecimal, volume: BigDecimal,
                 timestamp: Instant, tpe: String,
                 bidoid: Option[Long] = None, askoid: Option[Long] = None)
object Trade {
  val format = RecordFormat[Trade]
}

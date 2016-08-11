package co.coinsmith.kafka.cryptocoin

import java.time.Instant

import co.coinsmith.kafka.cryptocoin.avro.InstantTypeMaps._
import com.sksamuel.avro4s.RecordFormat


case class Tick(last: BigDecimal, bid: BigDecimal, ask: BigDecimal, timestamp: Instant,
                high: Option[BigDecimal], low: Option[BigDecimal], open: Option[BigDecimal],
                volume: Option[BigDecimal], vwap: Option[BigDecimal])
object Tick {
  val format = RecordFormat[Tick]

  def apply(last: String, bid: String, ask: String, timestamp: Instant,
            high: Option[String] = None, low: Option[String] = None, open: Option[String] = None,
            volume: Option[String] = None, vwap: Option[String] = None) =
    new Tick(BigDecimal(last), BigDecimal(bid), BigDecimal(ask), timestamp, high map { BigDecimal(_) }, low map { BigDecimal(_) }, open map { BigDecimal(_) }, volume map { BigDecimal(_) }, vwap map { BigDecimal(_) })
}

case class Order(price: BigDecimal, volume: BigDecimal, timestamp: Option[Instant] = None)
object Order {
  def apply(price: String, volume: String) = new Order(BigDecimal(price), BigDecimal(volume))
}

case class OrderBook(bids: List[Order], asks: List[Order], timestamp: Option[Instant] = None)
object OrderBook {
  val format = RecordFormat[OrderBook]
}

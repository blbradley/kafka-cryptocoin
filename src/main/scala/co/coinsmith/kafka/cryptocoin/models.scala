package co.coinsmith.kafka.cryptocoin

import java.time.Instant

import co.coinsmith.kafka.cryptocoin.avro.InstantTypeMaps._
import com.sksamuel.avro4s.RecordFormat


case class Tick(last: BigDecimal, bid: BigDecimal, ask: BigDecimal,
                high: BigDecimal, low: BigDecimal, open: Option[BigDecimal],
                volume: BigDecimal, vwap: Option[BigDecimal], timestamp: Instant)
object Tick {
  val format = RecordFormat[Tick]

  def apply(last: String, bid: String, ask: String,
            high: String, low: String, open: Option[String],
            volume: String, vwap: Option[String], timestamp: Instant) =
    new Tick(BigDecimal(last), BigDecimal(bid), BigDecimal(ask),
             BigDecimal(high), BigDecimal(low), open map { BigDecimal(_) },
             BigDecimal(volume), vwap map { BigDecimal(_) }, timestamp)
}

case class Order(price: BigDecimal, volume: BigDecimal, timestamp: Option[BigDecimal] = None)
object Order {
  def apply(price: String, volume: String) = new Order(BigDecimal(price), BigDecimal(volume))
}

case class OrderBook(bids: List[Order], asks: List[Order], timestamp: Option[Instant] = None)
object OrderBook {
  val format = RecordFormat[OrderBook]
}

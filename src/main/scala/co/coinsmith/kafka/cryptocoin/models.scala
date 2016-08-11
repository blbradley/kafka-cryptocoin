package co.coinsmith.kafka.cryptocoin

import java.time.Instant

import com.sksamuel.avro4s.RecordFormat
import co.coinsmith.kafka.cryptocoin.avro.InstantTypeMaps._


case class Tick(
  last: BigDecimal,
  bid: BigDecimal,
  ask: BigDecimal,
  high: BigDecimal,
  low: BigDecimal,
  vwap: BigDecimal,
  volume: BigDecimal,
  open: BigDecimal,
  timestamp: String
)
object Tick {
  val format = RecordFormat[Tick]
}

case class Order(price: BigDecimal, volume: BigDecimal, timestamp: Option[BigDecimal] = None)
object Order {
  def apply(price: String, volume: String) = new Order(BigDecimal(price), BigDecimal(volume))
}
case class OrderBook(bids: List[Order], asks: List[Order], timestamp: Option[Instant])
object OrderBook {
  val format = RecordFormat[OrderBook]
}

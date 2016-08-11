package co.coinsmith.kafka.cryptocoin

import com.sksamuel.avro4s.RecordFormat


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

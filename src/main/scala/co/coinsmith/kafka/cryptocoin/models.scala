package co.coinsmith.kafka.cryptocoin

import com.sksamuel.avro4s.RecordFormat


case class Tick(
  high: BigDecimal,
  last: BigDecimal,
  timestamp: String,
  bid: BigDecimal,
  vwap: BigDecimal,
  volume: BigDecimal,
  low: BigDecimal,
  ask: BigDecimal,
  open: BigDecimal
)
object Tick {
  val format = RecordFormat[Tick]
}

case class Order(price: BigDecimal, volume: BigDecimal, timestamp: Option[BigDecimal] = None)

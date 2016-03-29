package co.coinsmith.kafka.cryptocoin

import java.time.Instant

import org.json4s.JsonDSL.WithBigDecimal._


object Utils {
  def orderBookToJson(
    timestamp: Option[String],
    timeCollected: Instant,
    asks: List[Order],
    bids: List[Order]) = {
    ("timestamp" -> timestamp) ~
      ("time_collected" -> timeCollected.toString) ~
      ("ask_prices" -> asks.map { o => o.price }) ~
      ("ask_volumes" -> asks.map { o => o.volume }) ~
      ("bid_prices" -> bids.map { o => o.price }) ~
      ("bid_volumes" -> bids.map { o => o.volume })
  }
}

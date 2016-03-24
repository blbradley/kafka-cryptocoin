package co.coinsmith.kafka.cryptocoin

import org.json4s.JsonDSL.WithBigDecimal._


object Utils {
  def orderBookToJson(
    timestamp: Option[Long],
    timeCollected: Long,
    asks: List[List[BigDecimal]],
    bids: List[List[BigDecimal]]) = {
    ("timestamp" -> timestamp) ~
      ("time_collected" -> timeCollected) ~
      ("ask_prices" -> asks.map { o => o(0) }) ~
      ("ask_volumes" -> asks.map { o => o(1) }) ~
      ("bid_prices" -> bids.map { o => o(0) }) ~
      ("bid_volumes" -> bids.map { o => o(1) })
  }
}

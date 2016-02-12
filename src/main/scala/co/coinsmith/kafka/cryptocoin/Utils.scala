package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import java.util.Date

import com.xeiam.xchange.dto.marketdata.OrderBook
import com.xeiam.xchange.dto.trade.LimitOrder
import org.json4s.JsonDSL.WithBigDecimal._


object Utils {
  def limitOrdersToLists(orderList: List[LimitOrder]) = {
    orderList.map { o => List(o.getLimitPrice, o.getTradableAmount)}
      .map { _ map { _.stripTrailingZeros } }
      .map { _ map { BigDecimal(_) } }
  }

  def orderBookToJson(ob: OrderBook, timeCollected: Long) = {
    val bids = limitOrdersToLists(ob.getBids.toList)
    val asks = limitOrdersToLists(ob.getAsks.toList)
    val timestamp = Option(ob.getTimeStamp) match {
      case Some(ts: Date) => Some(ts.getTime)
      case _ => None
    }

    ("timestamp" -> timestamp) ~
      ("time_collected" -> timeCollected) ~
      ("ask_prices" -> asks.map { o => o(0) }) ~
      ("ask_volumes" -> asks.map { o => o(1) }) ~
      ("bid_prices" -> bids.map { o => o(0) }) ~
      ("bid_volumes" -> bids.map { o => o(1) })
  }
}

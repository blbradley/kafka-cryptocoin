package co.coinsmith.kafka.cryptocoin

import scala.collection.JavaConversions._
import java.util.Date

import com.xeiam.xchange.currency.CurrencyPair
import com.xeiam.xchange.dto.marketdata.Ticker.Builder
import com.xeiam.xchange.dto.marketdata.{OrderBook, Ticker}
import com.xeiam.xchange.dto.trade.LimitOrder
import org.json4s.JsonAST._
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.{CustomSerializer, Extraction, NoTypeHints}


class TickerSerializer extends CustomSerializer[Ticker]( format => (
  {
    // we are not deserializing yet, but here is the boilerplate
    case _ =>
      val tick = new Builder
      tick.currencyPair(CurrencyPair.BTC_USD)
        .last(new java.math.BigDecimal(0))
        .bid(new java.math.BigDecimal(0))
        .ask(new java.math.BigDecimal(0))
        .high(new java.math.BigDecimal(0))
        .low(new java.math.BigDecimal(0))
        .vwap(new java.math.BigDecimal(0))
        .volume(new java.math.BigDecimal(0))
        .build
  },
  {
    case t: Ticker =>
      val vwap = Option(t.getVwap) match {
        case Some(d) => JDecimal(d)
        case None => JNull
      }
      JObject(
        JField("timestamp", JInt(t.getTimestamp.getTime)) ::
        JField("currencyPair", JString(t.getCurrencyPair.toString)) ::
        JField("last", JDecimal(t.getLast)) ::
        JField("bid", JDecimal(t.getBid)) ::
        JField("ask", JDecimal(t.getAsk)) ::
        JField("high", JDecimal(t.getHigh)) ::
        JField("low", JDecimal(t.getLow)) ::
        JField("vwap", vwap) ::
        JField("volume", JDecimal(t.getVolume)) ::
        Nil
      )
  })
)

object Utils {
  implicit val formats = Serialization.formats(NoTypeHints) + new TickerSerializer

  def tickerToJson(t: Ticker, timeCollected: Long) = {
    Extraction.decompose(t) merge render("time_collected" -> timeCollected)
  }

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

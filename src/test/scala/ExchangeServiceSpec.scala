package co.coinsmith.kafka.cryptocoin

import com.xeiam.xchange.bitfinex.v1.BitfinexExchange
import com.xeiam.xchange.bitstamp.BitstampExchange


class ExchangeServiceSpec extends KafkaCryptocoinFunSpec {
  describe("ExchangeService") {
    it("should load all exchanges") {
      val expected = Set(classOf[BitfinexExchange], classOf[BitstampExchange])
      val result = ExchangeService.getExchanges.map(_.getClass).toSet
      assert(expected == result)
    }
  }
}

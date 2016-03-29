package co.coinsmith.kafka.cryptocoin


case class Order(price: BigDecimal, volume: BigDecimal, timestamp: Option[BigDecimal] = None)

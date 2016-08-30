package co.coinsmith.kafka.cryptocoin.avro

import com.sksamuel.avro4s.ScaleAndPrecision

object GlobalScaleAndPrecision {
  implicit val sp = ScaleAndPrecision(8, 20)
}

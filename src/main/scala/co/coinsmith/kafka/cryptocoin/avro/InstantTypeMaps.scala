package co.coinsmith.kafka.cryptocoin.avro

import java.time.Instant

import com.sksamuel.avro4s.{FromValue, ToSchema, ToValue}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field


object InstantTypeMaps {
  implicit object InstantToSchema extends ToSchema[Instant] {
    override val schema: Schema = Schema.create(Schema.Type.STRING)
  }

  implicit object InstantToValue extends ToValue[Instant] {
    override def apply(value: Instant): String = value.toString
  }

  implicit object InstantFromValue extends FromValue[Instant] {
    override def apply(value: Any, field: Field): Instant = Instant.parse(value.toString)
  }
}

package polling

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.testkit.TestActorRef
import akka.util.ByteString
import co.coinsmith.kafka.cryptocoin.polling.OKCoinPollingActor
import org.json4s.JsonDSL.WithBigDecimal._
import org.json4s.jackson.JsonMethods._


class OKCoinPollingActorSpec
  extends HTTPPollingActorSpec(ActorSystem("OKCoinPollingActorSpecSystem")) {
  implicit val materializer = ActorMaterializer()

  val actorRef = TestActorRef[OKCoinPollingActor]
  val actor = actorRef.underlyingActor

  "OKCoinPollingActor" should "process a ticker message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("date" -> "1462489329") ~ ("ticker" ->
      ("buy" -> "2906.58") ~
        ("high" -> "2915.0")  ~
        ("last" -> "2906.64") ~
        ("low" -> "2885.6") ~
        ("sell" -> "2906.63") ~
        ("vol" ->"635178.4712"))
    val contentType = ContentTypes.`text/html(UTF-8)`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Strict(contentType, data)

    val expected = ("time_collected" -> "1970-01-01T00:00:10Z") ~
      ("timestamp" -> "2016-05-05T23:02:09Z") ~
      ("bid" -> 2906.58) ~
      ("high" -> 2915.0) ~
      ("last" -> 2906.64) ~
      ("low" -> 2885.6) ~
      ("ask" -> 2906.63) ~
      ("volume" -> 635178.4712)

    val (pub, sub) = testExchangeFlowPubSub(actor.tickFlow).run()
    pub.sendNext((timeCollected, entity))
    sub.requestNext(("ticks", expected))
  }

  it should "process an orderbook message" in {
    val timeCollected = Instant.ofEpochSecond(10L)
    val json = ("asks" -> List(
      List(2915.15, 0.032),
      List(2915.14, 1.701),
      List(2915.08,0.172)
    )) ~ ("bids" -> List(
      List(2906.83, 1.912),
      List(2906.76, 0.01),
      List(2906.75, 0.082)
    ))
    val contentType = ContentTypes.`text/html(UTF-8)`
    val data = ByteString(compact(render(json)))
    val entity = HttpEntity.Strict(contentType, data)

    val expected = ("time_collected" -> timeCollected.toString) ~
      ("asks" -> List(
        List(2915.15, 0.032),
        List(2915.14, 1.701),
        List(2915.08, 0.172)
      )) ~ ("bids" -> List(
        List(2906.83, 1.912),
        List(2906.76, 0.01),
        List(2906.75, 0.082)
      ))
    
    val (pub, sub) = testExchangeFlowPubSub(actor.orderbookFlow).run()
    pub.sendNext((timeCollected, entity))
    sub.requestNext(("orderbook", expected))
  }
}

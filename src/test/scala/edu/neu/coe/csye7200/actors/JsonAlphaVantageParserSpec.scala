package edu.neu.coe.csye7200.actors

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.http.scaladsl.model._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.io.Source

/**
  * Unlike JsonYQLParserSpec/JsonGoogleParserSpec/JsonGoogleOptionParserSpec, this "send back"
  * test uses a plain TestProbe[HedgeFundCommand] as the blackboard directly, rather than a mock
  * blackboard wired up to real MarketData/UpdateLogger children -- since the only thing this
  * spec needs to prove is that EntityParser -> JsonAlphaVantageParser correctly decodes a
  * GLOBAL_QUOTE fixture into a KnowledgeUpdate, not exercise the full query/reply round trip.
  */
class JsonAlphaVantageParserSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  val json: String = Source.fromFile(getClass.getResource("/alphaVantageExample.json").getPath).mkString

  "json conversion" in {
    val entity: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    JsonAlphaVantageParser.decode(entity) match {
      case Right(response) =>
        response.get("Global Quote") match {
          case Some(quote) =>
            quote.get("01. symbol") should matchPattern { case Some("MSFT") => }
            quote.get("05. price") should matchPattern { case Some("412.50") => }
          case None => fail("'Global Quote' missing from decoded response")
        }
      case Left(x) => fail("decoding error: " + x)
    }
  }

  "send back" in {
    val probe = testKit.createTestProbe[HedgeFundCommand]()
    val entityParser = testKit.spawn(EntityParser(probe.ref))
    val entity: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    entityParser ! EntityMessage("json:AV", entity)
    val msg = probe.expectMessageType[KnowledgeUpdate](3.seconds)
    msg.symbol shouldEqual "MSFT"
    msg.update.get("05. price") should matchPattern { case Some("412.50") => }
  }
}

package edu.neu.coe.csye7200.actors

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.http.scaladsl.model._
import edu.neu.coe.csye7200.providers.FinnhubProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.io.Source
import scala.util.Success

/**
  * Unlike JsonAlphaVantageParserSpec, there's no "send back" test here that expects a
  * KnowledgeUpdate: Finnhub's /quote response never identifies the symbol it quotes, so (as
  * documented on JsonFinnhubParser) the broadcast-style EntityParser pipeline has nothing to
  * correlate a response to a symbol with, and deliberately just logs a warning and drops the
  * message instead. FinnhubProvider.decodePrice (used by the live cache.PriceCacheActor path,
  * which already knows its symbol) is what actually needs to work, and is tested here directly.
  */
class JsonFinnhubParserSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  val json: String = Source.fromFile(getClass.getResource("/finnhubExample.json").getPath).mkString

  "json conversion" in {
    val entity: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    JsonFinnhubParser.decode(entity) match {
      case Right(response) =>
        response.get("c") should matchPattern { case Some(412.50) => }
        response.get("pc") should matchPattern { case Some(409.00) => }
      case Left(x) => fail("decoding error: " + x)
    }
  }

  "not forward an update to the blackboard (no symbol in the response to correlate by)" in {
    val probe = testKit.createTestProbe[HedgeFundCommand]()
    val entityParser = testKit.spawn(EntityParser(probe.ref))
    val entity: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    entityParser ! EntityMessage("json:FH", entity)
    probe.expectNoMessage()
  }

  "FinnhubProvider.decodePrice extracts the current price" in {
    val entity: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    FinnhubProvider.decodePrice("MSFT", entity) shouldEqual Success(412.50)
  }
}

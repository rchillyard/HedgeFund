package edu.neu.coe.csye7200.actors

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model._
import akka.util.Timeout
import edu.neu.coe.csye7200.actors.JsonGoogleParser.Results
import edu.neu.coe.csye7200.model.Model
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * This specification really tests much of the HedgeFund app but because it particularly deals with
  * processing data from the YQL (Yahoo Query Language) using JSON, we call it by its given name.
  */
class JsonGoogleParserSpec extends AnyWordSpecLike with Matchers with Inside with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  val json: String = Source.fromFile(getClass.getResource("/googleExample.json").getPath).mkString

  "json read" in {
    import JsonGoogleParser.MyJsonProtocol._
    import spray.json._
    val obj: Results = JsonGoogleParser.fix(json).parseJson.convertTo[Results]
    obj shouldBe List(Map("e" -> Some("NASDAQ"), "elt" -> Some("Jul 24, 7:15PM EDT"), "s" -> Some("2"), "ec" -> Some("+0.05"), "cp_fix" -> Some("-0.53"), "l_cur" -> Some("124.50"), "ccol" -> Some("chr"), "t" -> Some("AAPL"), "el" -> Some("124.55"), "yld" -> Some("1.67"), "div" -> Some("0.52"), "pcls_fix" -> Some("125.16"), "el_cur" -> Some("124.55"), "id" -> Some("22144"), "ec_fix" -> Some("0.05"), "l" -> Some("124.50"), "el_fix" -> Some("124.55"), "l_fix" -> Some("124.50"), "ecp_fix" -> Some("0.04"), "c_fix" -> Some("-0.66"), "c" -> Some("-0.66"), "eccol" -> Some("chg"), "cp" -> Some("-0.53"), "lt" -> Some("Jul 24, 4:08PM EDT"), "ecp" -> Some("0.04"), "lt_dts" -> Some("2015-07-24T16:08:30Z"), "ltt" -> Some("4:08PM EDT")), Map("e" -> Some("NASDAQ"), "elt" -> Some("Jul 24, 6:34PM EDT"), "s" -> Some("2"), "ec" -> Some("+0.02"), "cp_fix" -> Some("-0.92"), "l_cur" -> Some("38.85"), "ccol" -> Some("chr"), "t" -> Some("YHOO"), "el" -> Some("38.87"), "yld" -> Some(""), "div" -> Some(""), "pcls_fix" -> Some("39.21"), "el_cur" -> Some("38.87"), "id" -> Some("658890"), "ec_fix" -> Some("0.02"), "l" -> Some("38.85"), "el_fix" -> Some("38.87"), "l_fix" -> Some("38.85"), "ecp_fix" -> Some("0.06"), "c_fix" -> Some("-0.36"), "c" -> Some("-0.36"), "eccol" -> Some("chg"), "cp" -> Some("-0.92"), "lt" -> Some("Jul 24, 4:08PM EDT"), "ecp" -> Some("0.06"), "lt_dts" -> Some("2015-07-24T16:08:28Z"), "ltt" -> Some("4:08PM EDT")))
  }

  "json conversion" in {
    val contentTypeText = ContentType(MediaTypes.`text/html`, HttpCharsets.`ISO-8859-1`)
    val entity: HttpEntity.Strict = HttpEntity(contentTypeText, json.getBytes())
    val ok = JsonGoogleParser.decode(entity) match {
      case Right(x) =>
        x.length should equal(2)
        val quotes = x
        quotes.head.get("t") should matchPattern { case Some(Some("AAPL")) => }

      case Left(x) =>
        fail("decoding error: " + x)
    }
    ok shouldBe Succeeded
  }

  "send back" in {
    val probe = testKit.createTestProbe[QueryResponse]()
    val blackboard = testKit.spawn(MockGoogleBlackboard(probe.ref))
    val contentType = ContentType(MediaTypes.`text/html`, HttpCharsets.`ISO-8859-1`)
    val entityParser = testKit.spawn(EntityParser(blackboard))
    val entity: HttpEntity.Strict = HttpEntity(contentType, json.getBytes())
    entityParser ! EntityMessage("json:GF", entity)
    val msg = probe.expectMessageType[QueryResponse](3.seconds)
    println("msg received: " + msg)
    msg should matchPattern {
      case QueryResponse("AAPL", _) =>
    }
    inside(msg) {
      case QueryResponse(_, attributes) => attributes.get("l") should matchPattern { case Some("124.50") => }
    }
  }
}

object MockGoogleUpdateLogger {
  def apply(marketData: ActorRef[MarketDataCommand], probe: ActorRef[QueryResponse]): Behavior[UpdateLoggerCommand] =
    Behaviors.setup { context =>
      implicit val timeout: Timeout = Timeout(5.seconds)

      Behaviors.receiveMessage {
        case Confirmation(identifier, model, _) =>
          model.getKey("price") match {
            case Some(p) =>
              context.ask(marketData, (replyTo: ActorRef[QueryResponse]) => SymbolQuery(identifier, List(p), replyTo)) {
                case Success(result) => SymbolQueryResult(identifier, Success(result))
                case Failure(ex) => SymbolQueryResult(identifier, Failure(ex))
              }
            case None => context.log.warn(s"'price' not defined in model")
          }
          Behaviors.same

        case SymbolQueryResult(identifier, Success(result)) =>
          result.attributes foreach {
            case (k, v) => context.log.info(s"$identifier attribute $k has been updated to: $v")
          }
          probe ! result
          Behaviors.same

        case SymbolQueryResult(_, Failure(ex)) =>
          context.log.warn(ex.getLocalizedMessage)
          Behaviors.same

        case _: PortfolioUpdate | _: OptionQueryResult => Behaviors.same
      }
    }
}

object MockGoogleBlackboard {
  def apply(probe: ActorRef[QueryResponse]): Behavior[HedgeFundCommand] = Behaviors.setup { context =>
    val marketData: ActorRef[MarketDataCommand] = context.spawn(MarketData(context.self), "marketData")
    val updateLogger: ActorRef[UpdateLoggerCommand] = context.spawn(MockGoogleUpdateLogger(marketData, probe), "updateLogger")

    Behaviors.receiveMessage {
      case m: MarketDataCommand =>
        marketData ! m
        Behaviors.same
      case c@Confirmation("AAPL", _, _) =>
        updateLogger ! c
        Behaviors.same
      case _: Confirmation => Behaviors.same
      case _ => Behaviors.same
    }
  }
}

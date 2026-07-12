package edu.neu.coe.csye7200.actors

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model._
import akka.util.Timeout
import edu.neu.coe.csye7200.model.Model
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, Inside, Succeeded}

import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * This specification really tests much of the HedgeFund app but because it particularly deals with
  * processing data from the YQL (Yahoo Query Language) using JSON, we call it by its given name.
  */
class JsonYQLParserSpec extends AnyWordSpecLike with Matchers with Inside with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  val json: String = Source.fromFile(getClass.getResource("/yqlExample.json").getPath).mkString

  "json conversion" in {
    val body: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    val ok = JsonYQLParser.decode(body) match {
      case Right(x) =>
        val count = x.query.count
        count should equal(4)
        x.query.results.quote.length should equal(count)
        x.query.results.get(count - 1, "symbol") should matchPattern { case Some("MSFT") => }

      case Left(x) =>
        fail("decoding error: " + x)
    }
    ok shouldBe Succeeded
  }

  "send back" in {
    val probe = testKit.createTestProbe[QueryResponse]()
    val blackboard = testKit.spawn(MockYQLBlackboard(probe.ref))
    val entityParser = testKit.spawn(EntityParser(blackboard))
    val entity: HttpEntity.Strict = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    entityParser ! EntityMessage("json:YQL", entity)
    val msg = probe.expectMessageType[QueryResponse](3.seconds)
    println("msg received: " + msg)
    msg should matchPattern {
      case QueryResponse("MSFT", _) =>
    }
    inside(msg) {
      case QueryResponse(_, attributes) => attributes.get("Ask") should matchPattern { case Some("46.17") => }
    }
  }

}

object MockYQLUpdateLogger {
  def apply(marketData: ActorRef[MarketDataCommand], probe: ActorRef[QueryResponse]): Behavior[UpdateLoggerCommand] =
    Behaviors.setup { context =>
      implicit val timeout: Timeout = Timeout(5.seconds)

      Behaviors.receiveMessage {
        case Confirmation(identifier, model, _) =>
          // sender is the MarketData actor
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

object MockYQLBlackboard {
  def apply(probe: ActorRef[QueryResponse]): Behavior[HedgeFundCommand] = Behaviors.setup { context =>
    val marketData: ActorRef[MarketDataCommand] = context.spawn(MarketData(context.self), "marketData")
    val updateLogger: ActorRef[UpdateLoggerCommand] = context.spawn(MockYQLUpdateLogger(marketData, probe), "updateLogger")

    Behaviors.receiveMessage {
      case m: MarketDataCommand =>
        marketData ! m
        Behaviors.same
      // Cut down on the volume of messages
      case c@Confirmation("MSFT", _, _) =>
        updateLogger ! c
        Behaviors.same
      case _: Confirmation => Behaviors.same
      case _ => Behaviors.same
    }
  }
}

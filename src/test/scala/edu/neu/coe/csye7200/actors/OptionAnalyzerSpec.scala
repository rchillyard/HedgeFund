package edu.neu.coe.csye7200.actors

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.model.GoogleOptionModel
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfterAll, Inside}

import scala.concurrent.duration._

/**
  * This specification really tests much of the HedgeFund app but because it particularly deals with
  * processing data from the YQL (Yahoo Query Language) using JSON, we call it by its given name.
  */
class OptionAnalyzerSpec extends AnyWordSpecLike with Matchers with Inside with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "send back" taggedAs Slow in {
    val model = new GoogleOptionModel()
    val confirmationProbe = testKit.createTestProbe[Confirmation]()
    val queryProbe = testKit.createTestProbe[QueryResponse]()
    val blackboard = testKit.spawn(MockAnalyzerBlackboard(confirmationProbe.ref))
    blackboard ! CandidateOption(model, "XX375", put = true, Map("strike" -> "45.2"), Map("underlying_id" -> "1234", "Sharpe" -> 0.45))
    val confirmationMsg = confirmationProbe.expectMessageType[Confirmation](3.seconds)
    println("confirmation msg received: " + confirmationMsg)
    inside(confirmationMsg) {
      case Confirmation(id, m, details) =>
        println(s"confirmation1 details: $details")
        id shouldEqual "XX375"
        blackboard ! KnowledgeUpdate(m, "XX", Map("id" -> "1234"))
        val confirmationMsg2 = confirmationProbe.expectMessageType[Confirmation](3.seconds)
        println("confirmation msg2 received: " + confirmationMsg2)
        // Note that the key "id" is in the model for symbols, not options
        blackboard ! OptionQuery("id", "1234", queryProbe.ref)
        val responseMsg = queryProbe.expectMessageType[QueryResponse](3.seconds)
        println("msg received: " + responseMsg)
        inside(responseMsg) {
          case QueryResponse(symbol, attributes) =>
            symbol shouldEqual "XX"
            println(s"attributes: $attributes")
        }
    }
  }
}

object MockAnalyzerBlackboard {
  // UpdateLogger is deliberately not spawned here: Confirmation is intercepted below before it would
  // ever reach "updateLogger" routing, exactly as in the original mock's forward-to-testActor override.
  def apply(confirmationProbe: ActorRef[Confirmation]): Behavior[HedgeFundCommand] = Behaviors.setup { context =>
    val marketData: ActorRef[MarketDataCommand] = context.spawn(MarketData(context.self), "marketData")
    val optionAnalyzer: ActorRef[OptionAnalyzerCommand] = context.spawn(OptionAnalyzer(context.self), "optionAnalyzer")

    Behaviors.receiveMessage {
      case m: MarketDataCommand =>
        marketData ! m
        Behaviors.same
      case m: OptionAnalyzerCommand =>
        optionAnalyzer ! m
        Behaviors.same
      case c: Confirmation =>
        confirmationProbe ! c
        Behaviors.same
      case _ => Behaviors.same
    }
  }
}

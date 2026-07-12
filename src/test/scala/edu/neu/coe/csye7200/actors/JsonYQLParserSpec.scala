package edu.neu.coe.csye7200.actors

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.http.scaladsl.model._
import akka.testkit._
import edu.neu.coe.csye7200.model.Model
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.io.Source

/**
  * This specification really tests much of the HedgeFund app but because it particularly deals with
  * processing data from the YQL (Yahoo Query Language) using JSON, we call it by its given name.
  */
class JsonYQLParserSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with AnyWordSpecLike with Matchers with Inside with BeforeAndAfterAll {

  def this() = this(ActorSystem("JsonYQLParserSpec"))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  import scala.language.postfixOps

  val json: String = Source.fromFile(getClass.getResource("/yqlExample.json").getPath) mkString

  "json conversion" in {
    val body = HttpEntity(MediaTypes.`application/json`, json.getBytes())
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
    val blackboard = system.actorOf(Props.create(classOf[MockYQLBlackboard], testActor), "blackboard")
    val entityParser = _system.actorOf(Props.create(classOf[EntityParser], blackboard), "entityParser")
    val entity = HttpEntity(MediaTypes.`application/json`, json.getBytes())
    entityParser ! EntityMessage("json:YQL", entity)
    val msg = expectMsgClass(3.seconds, classOf[QueryResponse])
    println("msg received: " + msg)
    msg should matchPattern {
      case QueryResponse("MSFT", _) =>
    }
    inside(msg) {
      case QueryResponse(_, attributes) => attributes.get("Ask") should matchPattern { case Some("46.17") => }
    }
  }

}

import akka.pattern.ask

import scala.concurrent.Await

class MockYQLUpdateLogger(blackboard: ActorRef) extends UpdateLogger(blackboard) {
  override def processStock(identifier: String, model: Model): Unit = {
    model.getKey("price") match {
      case Some(p) =>
        // sender is the MarketData actor
        val future = sender() ? SymbolQuery(identifier, List(p))
        val result = Await.result(future, timeout.duration).asInstanceOf[QueryResponse]
        result.attributes foreach {
          case (k, v) =>
            log.info(s"$identifier attribute $k has been updated to: $v")
            blackboard ! result
        }
      case None => log.warning(s"'price' not defined in model")
    }
  }
}

class MockYQLBlackboard(testActor: ActorRef) extends Blackboard(Map(classOf[KnowledgeUpdate] -> "marketData", classOf[SymbolQuery] -> "marketData", classOf[OptionQuery] -> "marketData", classOf[CandidateOption] -> "optionAnalyzer", classOf[Confirmation] -> "updateLogger"),
  Map("marketData" -> classOf[MarketData], "optionAnalyzer" -> classOf[OptionAnalyzer], "updateLogger" -> classOf[MockYQLUpdateLogger])) {

  override def receive: PartialFunction[Any, Unit] = {
    case msg: Confirmation => msg match {
      // Cut down on the volume of messages
      case Confirmation("MSFT", _, _) => super.receive(msg)
      case _ =>
    }
    case msg: QueryResponse => testActor forward msg

    case msg => super.receive(msg)
  }
}


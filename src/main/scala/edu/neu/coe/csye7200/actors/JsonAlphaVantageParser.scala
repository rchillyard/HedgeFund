package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.model.{AlphaVantageModel, Model}

/**
  * Parses Alpha Vantage's `GLOBAL_QUOTE` response, which (unlike YQL/Google's bespoke shapes)
  * decodes directly as a `Map[String, Map[String, String]]` -- no custom case classes needed.
  *
  * @author robinhillyard
  */
object JsonAlphaVantageParser {

  private val globalQuoteKey = "Global Quote"

  import edu.neu.coe.csye7200.http.JsonUnmarshalling
  import edu.neu.coe.csye7200.http.JsonUnmarshalling.Deserialized
  import akka.http.scaladsl.model.HttpEntity

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = Behaviors.setup { context =>
    val model: Model = new AlphaVantageModel

    def processQuote(quote: Map[String, String]): Unit = model.getKey("symbol") match {
      case Some(s) =>
        quote.get(s) match {
          case Some(symbol) => blackboard ! KnowledgeUpdate(model, symbol, quote)
          case None => context.log.warn(s"symbol $s is undefined")
        }
      case None => context.log.warn("'symbol' is undefined in model")
    }

    Behaviors.receiveMessage {
      case ContentMessage(entity) =>
        context.log.debug("JsonAlphaVantageParser received ContentMessage")
        decode(entity) match {
          case Right(response) =>
            response.get(globalQuoteKey) match {
              case Some(quote) => processQuote(quote)
              case None => context.log.warn(s"'$globalQuoteKey' missing from response")
            }
          case Left(message) => context.log.warn(message.toString)
        }
        Behaviors.same
    }
  }

  object MyJsonProtocol extends spray.json.DefaultJsonProtocol with spray.json.NullOptions {
  }

  import MyJsonProtocol._

  type Response = Map[String, Map[String, String]]

  def decode(entity: HttpEntity.Strict): Deserialized[Response] = JsonUnmarshalling.decode[Response](entity.data)

}

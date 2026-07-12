package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model._
import akka.util.ByteString
import edu.neu.coe.csye7200.model.{GoogleModel, Model}

import scala.util._

/**
  * TODO create a super-type for this kind of actor
  *
  * @author robinhillyard
  */
object JsonGoogleParser {

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = Behaviors.setup { context =>
    val model: Model = new GoogleModel

    def processQuote(quotes: Seq[Map[String, Option[String]]]): Unit = quotes foreach { q => processInstrument(q) }

    def processInstrument(quote: Map[String, Option[String]]): Unit = model.getKey("symbol") match {
      case Some(s) =>
        quote.get(s) match {
          case Some(Some(symbol)) => updateMarket(symbol, quote)
          case _ => context.log.warn(s"$s is undefined in quote")
        }
      case None => context.log.warn("'symbol' is not defined in model")
    }

    def updateMarket(symbol: String, quote: Map[String, Option[String]]): Unit =
      blackboard ! KnowledgeUpdate(model, symbol, quote flatMap { case (k, Some(v)) => Option(k -> v); case _ => None })

    Behaviors.receiveMessage {
      case ContentMessage(entity) =>
        context.log.debug("JsonGoogleParser received ContentMessage")
        decode(entity) match {
          case Right(results) => processQuote(results)
          case Left(message) => context.log.warn("Decoding error: " + message)
        }
        Behaviors.same
    }
  }

  import edu.neu.coe.csye7200.http.JsonUnmarshalling
  import edu.neu.coe.csye7200.http.JsonUnmarshalling.Deserialized
  import edu.neu.coe.csye7200.http.MalformedContent
  import spray.json.{DefaultJsonProtocol, _}

  type Results = Seq[Map[String, Option[String]]]

  object MyJsonProtocol extends DefaultJsonProtocol with NullOptions {
  }

  import MyJsonProtocol._

  /**
    * This version of decode is a little more complex than usual because the Google
    * interface deliberately prefixes "//" to the start of the Json in order
    * that we should not be able to invoke the service without some effort.
    *
    * @param entity the entity extracted from the Http Response
    * @return the deserialized version
    */
  def decode(entity: HttpEntity.Strict): Deserialized[Results] =
    entity.contentType.mediaType match {
      case MediaTypes.`application/json` =>
        JsonUnmarshalling.decode[Results](entity.data)
      case MediaTypes.`text/html` =>
        JsonUnmarshalling.decode[Results](fix(entity.data))
      case x => Left(MalformedContent(s"logic error: contentType=$x"))
    }

  def fix(data: ByteString): String = fix(data.utf8String)

  def fix(s: String): String = s.substring(3)

}
package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.model.{Model, YQLModel}

import scala.util._

/**
  * TODO create a super-type for this kind of actor
  *
  * @author robinhillyard
  */
object JsonYQLParser {

  import edu.neu.coe.csye7200.http.JsonUnmarshalling
  import edu.neu.coe.csye7200.http.JsonUnmarshalling.Deserialized
  import akka.http.scaladsl.model.HttpEntity
  import spray.json.{DefaultJsonProtocol, _}

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = Behaviors.setup { context =>
    val model: Model = new YQLModel

    def processQuote(quotes: Seq[Map[String, Option[String]]]): Unit = quotes foreach { q => processInstrument(q) }

    def processInstrument(quote: Map[String, Option[String]]): Unit = model.getKey("symbol") match {
      case Some(s) =>
        quote.get(s) match {
          case Some(Some(symbol)) => updateMarket(symbol, quote)
          case _ => context.log.warn(s"symbol $s is undefined")
        }
      case _ => context.log.warn("'symbol' is undefined in model")
    }

    def updateMarket(symbol: String, quote: Map[String, Option[String]]): Unit =
      blackboard ! KnowledgeUpdate(model, symbol, quote flatMap { case (k, Some(v)) => Option(k -> v); case _ => None })

    Behaviors.receiveMessage {
      case ContentMessage(entity) =>
        context.log.debug("JsonYQLParser received ContentMessage")
        decode(entity) match {
          case Right(response) => processQuote(response.query.results.quote)
          case Left(message) => context.log.warn(message.toString)
        }
        Behaviors.same
    }
  }

  case class Response(query: Query)

  case class Query(count: Int, created: String, lang: String, diagnostics: Option[Diagnostics], results: Results)

  case class Diagnostics(url: Seq[Map[String, String]], publiclyCallable: String, `user-time`: String, `service-time`: String, `build-version`: String, query: DiagnosticsQuery,
                         cache: DiagnosticsCache, javascript: DiagnosticsJavascript)

  case class DiagnosticsQuery(`execution-start-time`: String, `execution-stop-time`: String, `execution-time`: String, params: String, content: String)

  case class DiagnosticsCache(`execution-start-time`: String, `execution-stop-time`: String, `execution-time`: String, method: String, `type`: String, content: String)

  case class DiagnosticsJavascript(`execution-start-time`: String, `execution-stop-time`: String, `execution-time`: String, `instructions-used`: String, `table-name`: String)

  case class Results(quote: Seq[Map[String, Option[String]]]) {
    def get(index: Int, key: String): Option[String] = {
      Try {
        quote(index)
      } match {
        case Success(y) => y.get(key) match {
          case Some(x) => x;
          case None => None
        }
        case Failure(_) => None
      }
    }
  }

  object MyJsonProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val diagnosticsQueryFormat: RootJsonFormat[DiagnosticsQuery] = jsonFormat5(DiagnosticsQuery)
    implicit val diagnosticsCacheFormat: RootJsonFormat[DiagnosticsCache] = jsonFormat6(DiagnosticsCache)
    implicit val diagnosticsJavascriptFormat: RootJsonFormat[DiagnosticsJavascript] = jsonFormat5(DiagnosticsJavascript)
    implicit val diagnosticsFormat: RootJsonFormat[Diagnostics] = jsonFormat8(Diagnostics)
    implicit val resultsFormat: RootJsonFormat[Results] = jsonFormat1(Results)
    implicit val queryFormat: RootJsonFormat[Query] = jsonFormat5(Query)
    implicit val entityFormat: RootJsonFormat[Response] = jsonFormat1(Response)
  }

  import MyJsonProtocol._

  def decode(entity: HttpEntity.Strict): Deserialized[Response] = JsonUnmarshalling.decode[Response](entity.data)

}

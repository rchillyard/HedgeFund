package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.client.RequestBuilding
import akka.http.scaladsl.model._
import edu.neu.coe.csye7200.http.HttpTransaction
import edu.neu.coe.csye7200.model.Model
import edu.neu.coe.csye7200.portfolio.Portfolio

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * @author robinhillyard
  *
  */
sealed trait HedgeFundCommand

sealed trait HttpReaderCommand extends HedgeFundCommand

sealed trait MarketDataCommand extends HedgeFundCommand

sealed trait OptionAnalyzerCommand extends HedgeFundCommand

sealed trait UpdateLoggerCommand extends HedgeFundCommand

case class HttpResult(queryProtocol: String, request: HttpRequest, response: HttpResponse) extends HttpReaderCommand

case class KnowledgeUpdate(model: Model, symbol: String, update: Map[String, String]) extends MarketDataCommand

case class SymbolQuery(identifier: String, keys: List[String], replyTo: ActorRef[QueryResponse]) extends MarketDataCommand

case class OptionQuery(key: String, value: Any, replyTo: ActorRef[QueryResponse]) extends MarketDataCommand

case class CandidateOption(model: Model, identifier: String, put: Boolean, optionDetails: Map[String, String], chainDetails: Map[String, Any]) extends OptionAnalyzerCommand

case class Confirmation(identifier: String, model: Model, attributes: Map[String, Any]) extends UpdateLoggerCommand

case class PortfolioUpdate(portfolio: Portfolio) extends UpdateLoggerCommand

// Private continuations for UpdateLogger's context.ask-based query flow (see UpdateLogger.scala).
// Not part of the public HedgeFund protocol, but must live alongside UpdateLoggerCommand since
// Scala requires a sealed trait's subtypes to be defined in the same file.
private[actors] case class SymbolQueryResult(identifier: String, result: Try[QueryResponse]) extends UpdateLoggerCommand

private[actors] case class OptionQueryResult(identifier: String, result: Try[QueryResponse]) extends UpdateLoggerCommand

case class QueryResponse(identifier: String, attributes: Map[String, String])

case class ExternalLookup(queryProtocol: String, url: Uri) extends HedgeFundCommand

object HedgeFundBlackboard extends RequestBuilding {

  def apply(): Behavior[HedgeFundCommand] = Behaviors.setup { context =>
    implicit val system: akka.actor.typed.ActorSystem[_] = context.system
    implicit val ec: ExecutionContext = context.executionContext

    val httpReader: ActorRef[HttpReaderCommand] = context.spawn(HttpReader(context.self), "httpReader")
    val marketData: ActorRef[MarketDataCommand] = context.spawn(MarketData(context.self), "marketData")
    val optionAnalyzer: ActorRef[OptionAnalyzerCommand] = context.spawn(OptionAnalyzer(context.self), "optionAnalyzer")
    val updateLogger: ActorRef[UpdateLoggerCommand] = context.spawn(UpdateLogger(marketData), "updateLogger")

    Behaviors.receiveMessage {
      case m: HttpReaderCommand =>
        httpReader ! m
        Behaviors.same
      case m: MarketDataCommand =>
        marketData ! m
        Behaviors.same
      case m: OptionAnalyzerCommand =>
        optionAnalyzer ! m
        Behaviors.same
      case m: UpdateLoggerCommand =>
        updateLogger ! m
        Behaviors.same
      case ExternalLookup(protocol, url) =>
        context.log.debug(s"External lookup with protocol: $protocol and url: $url")
        HttpTransaction(protocol, Get(url), context.self)
        Behaviors.same
    }
  }
}

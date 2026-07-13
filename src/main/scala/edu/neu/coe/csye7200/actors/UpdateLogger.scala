package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import edu.neu.coe.csye7200.model.Model
import edu.neu.coe.csye7200.portfolio.{Contract, Portfolio, Position}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * CONSIDER renaming this as PortfolioManager
  *
  * @author robinhillyard
  */
object UpdateLogger {

  def apply(marketData: ActorRef[MarketDataCommand]): Behavior[UpdateLoggerCommand] = Behaviors.setup { context =>
    implicit val timeout: Timeout = Timeout(5.seconds)
    var portfolio: Portfolio = Portfolio("", Nil)

    def processStock(identifier: String, model: Model): Unit = {
      model.getKey("price") match {
        case Some(p) =>
          context.ask(marketData, (replyTo: ActorRef[QueryResponse]) => SymbolQuery(identifier, List(p), replyTo)) {
            case Success(response) => SymbolQueryResult(identifier, Success(response))
            case Failure(ex) => SymbolQueryResult(identifier, Failure(ex))
          }
        case None => context.log.warn(s"'price' not defined in model")
      }
    }

    def processOption(identifier: String, model: Model, attributes: Map[String, Any]): Unit = {
      val key = "underlying"
      attributes.get(key) match {
        case Some(value) =>
          context.ask(marketData, (replyTo: ActorRef[QueryResponse]) => OptionQuery("id", value, replyTo)) {
            case Success(response) => OptionQueryResult(identifier, Success(response))
            case Failure(ex) => OptionQueryResult(identifier, Failure(ex))
          }
        case None => context.log.warn(s"processOption: value not present for $key")
      }
    }

    def showPortfolio(): Unit = {
      println(s"Portfolio for ${portfolio.name}")
      portfolio.positions foreach showPosition
    }

    def showPosition(position: Position): Unit = {
      println(s"position for ${position.symbol}: quantity=${position.quantity}; options=")
      position.contracts foreach showContract
    }

    def showContract(contract: Contract): Unit = {
      println(s"contract: $contract")
    }

    Behaviors.receiveMessage {
      case Confirmation(id, model, attrs) =>
        context.log.debug(s"update for identifier: $id")
        if (model.isOption)
          processOption(id, model, attrs)
        else
          processStock(id, model)
        Behaviors.same

      case PortfolioUpdate(p) =>
        context.log.debug(s"portfolio update for: ${p.name}")
        portfolio = p
        showPortfolio()
        Behaviors.same

      case SymbolQueryResult(identifier, result) =>
        result match {
          case Success(r) => r.attributes foreach {
            case (k, v) => context.log.info(s"$identifier attribute $k has been updated to: $v")
          }
          case Failure(ex) => context.log.warn(ex.getLocalizedMessage)
        }
        Behaviors.same

      case OptionQueryResult(identifier, result) =>
        result match {
          case Success(r) => println(s"Action Required: re: qualifying option $identifier with underlying symbol: ${r.identifier} and attributes: ${r.attributes}")
          case Failure(ex) => context.log.warn(ex.getLocalizedMessage)
        }
        Behaviors.same
    }
  }
}

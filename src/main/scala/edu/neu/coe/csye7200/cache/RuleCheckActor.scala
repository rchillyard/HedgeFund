package edu.neu.coe.csye7200.cache

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import edu.neu.coe.csye7200.rules.Predicate
import edu.neu.coe.csye7200.portfolio.Portfolio
import org.slf4j.Logger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

sealed trait RuleCheckCommand

private case object CheckPortfolio extends RuleCheckCommand

private case class PriceChecked(symbol: String, result: Try[Double]) extends RuleCheckCommand

private case class PortfolioValueChecked(result: Try[Double]) extends RuleCheckCommand

/**
  * Periodically (every `interval`) re-checks each portfolio position's price via the per-symbol
  * price cache, evaluating the `buy`/`sell` rules (`stockRules.txt`) whenever a price has
  * changed since the last check. Also logs the portfolio's total resolved value each cycle,
  * reusing `Portfolio.value` directly (both `getPrice` calls per symbol in a cycle -- one for
  * the per-symbol rule check, one inside `Portfolio.value` for the total -- normally land on
  * the same `PriceCacheActor`'s `cached` state rather than triggering two HTTP fetches, since
  * they happen within the same cycle, well inside the cache's TTL).
  *
  * @author robinhillyard
  */
object RuleCheckActor {

  def apply(portfolio: Portfolio, priceCacheManager: ActorRef[GetPriceFor], interval: FiniteDuration, log: Logger): Behavior[RuleCheckCommand] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        implicit val system: akka.actor.typed.ActorSystem[_] = context.system
        implicit val ec: scala.concurrent.ExecutionContext = context.executionContext
        implicit val scheduler: akka.actor.typed.Scheduler = context.system.scheduler
        implicit val timeout: Timeout = Timeout(10.seconds)

        val rules: Map[String, Predicate] = StockRules.getRules(log)

        def getPrice(symbol: String): Future[Double] = PriceCacheManager.getPrice(priceCacheManager, symbol)

        def checkPortfolio(): Unit = {
          portfolio.positions.foreach { position =>
            context.pipeToSelf(getPrice(position.symbol)) { result => PriceChecked(position.symbol, result) }
          }
          context.pipeToSelf(portfolio.value(getPrice)) { result => PortfolioValueChecked(result) }
        }

        timers.startTimerWithFixedDelay(CheckPortfolio, interval)
        checkPortfolio() // check once immediately at startup rather than waiting a full interval

        def active(previousPrices: Map[String, Double]): Behavior[RuleCheckCommand] =
          Behaviors.receiveMessage {
            case CheckPortfolio =>
              checkPortfolio()
              Behaviors.same

            case PriceChecked(symbol, Success(price)) =>
              previousPrices.get(symbol) match {
                case Some(previous) if previous != price =>
                  evaluate(StockCandidate(symbol, price, previous), rules)
                case None =>
                  log.info(s"$symbol initial price: $price")
                case _ => // unchanged since last check -- nothing to evaluate
              }
              active(previousPrices + (symbol -> price))

            case PriceChecked(symbol, Failure(ex)) =>
              log.warn(s"price lookup failed for $symbol: {}", ex.getMessage)
              Behaviors.same

            case PortfolioValueChecked(Success(value)) =>
              println(s"Portfolio '${portfolio.name}' total value: $value")
              Behaviors.same

            case PortfolioValueChecked(Failure(ex)) =>
              log.warn("portfolio value check failed: {}", ex.getMessage)
              Behaviors.same
          }

        active(Map.empty)
      }
    }

  private def evaluate(candidate: StockCandidate, rules: Map[String, Predicate]): Unit = {
    val change = candidate.price - candidate.previousPrice
    rules.get("buy").foreach { r =>
      if (r(candidate).getOrElse(false)) println(s"BUY signal for ${candidate.symbol}: price=${candidate.price} (change=$change)")
    }
    rules.get("sell").foreach { r =>
      if (r(candidate).getOrElse(false)) println(s"SELL signal for ${candidate.symbol}: price=${candidate.price} (change=$change)")
    }
  }
}

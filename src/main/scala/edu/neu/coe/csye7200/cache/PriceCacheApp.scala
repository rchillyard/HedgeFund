package edu.neu.coe.csye7200.cache

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import edu.neu.coe.csye7200.portfolio.Portfolio
import edu.neu.coe.csye7200.providers.MarketDataProvider
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration

/**
  * Top-level guardian for the price-cache subsystem: spawns the `PriceCacheManager` and wires
  * it into the `RuleCheckActor`, which periodically re-checks portfolio prices and evaluates
  * `buy`/`sell` rules whenever a price has changed.
  *
  * @author robinhillyard
  */
object PriceCacheApp {

  def apply(portfolio: Portfolio, ttl: FiniteDuration, ruleCheckInterval: FiniteDuration, provider: MarketDataProvider, log: Logger): Behavior[Nothing] =
    Behaviors.setup[Nothing] { context =>
      val priceCacheManager = context.spawn(PriceCacheManager(ttl, provider), "priceCacheManager")
      context.spawn(RuleCheckActor(portfolio, priceCacheManager, ruleCheckInterval, log), "ruleCheckActor")
      Behaviors.empty
    }
}

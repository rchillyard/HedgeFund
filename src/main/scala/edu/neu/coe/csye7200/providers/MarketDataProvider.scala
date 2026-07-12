package edu.neu.coe.csye7200.providers

import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.actors.{ContentMessage, HedgeFundCommand}
import edu.neu.coe.csye7200.model.{Model, Query}

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * A single registration unit bundling everything needed to fetch and parse quotes from
  * one market-data source: the protocol tag, the request-building `Query`, the attribute-key
  * `Model`, and the JSON-parsing actor. See [[ProviderRegistry]] for how providers are
  * registered and selected.
  *
  * @author robinhillyard
  */
trait MarketDataProvider {
  def protocol: String

  def query: Query

  def model: Model

  def parserBehavior(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage]

  /**
    * Minimum gap `HedgeFund.startup` should leave between firing successive per-symbol
    * requests to this provider, to stay under its rate limit -- e.g. Alpha Vantage's free
    * tier allows only 1 request/second. Defaults to no gap, appropriate for providers with
    * no meaningful rate limit (or, like YQL/Google, that are permanently dead anyway).
    */
  def requestInterval: FiniteDuration = Duration.Zero
}

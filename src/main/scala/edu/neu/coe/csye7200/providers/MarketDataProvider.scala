package edu.neu.coe.csye7200.providers

import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.HttpEntity
import edu.neu.coe.csye7200.actors.{ContentMessage, HedgeFundCommand}
import edu.neu.coe.csye7200.model.{Model, Query}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Try}

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

  /**
    * Extracts a single price from a raw HTTP response for `symbol`, fetched via this provider's
    * own `query`. Used by `cache.PriceCacheActor`, which (unlike the legacy `HedgeFundBlackboard`
    * pipeline) already knows the symbol from its own closure state and so needs a direct
    * response-to-price decode rather than a payload-correlated one. Defaults to unsupported,
    * appropriate for providers with no live decode logic (YQL/Google, which are permanently dead
    * anyway).
    */
  def decodePrice(symbol: String, entity: HttpEntity.Strict): Try[Double] =
    Failure(new UnsupportedOperationException(s"$protocol does not support live price decoding"))
}

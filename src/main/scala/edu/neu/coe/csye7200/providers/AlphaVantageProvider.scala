package edu.neu.coe.csye7200.providers

import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.HttpEntity
import edu.neu.coe.csye7200.actors.{ContentMessage, HedgeFundCommand, JsonAlphaVantageParser}
import edu.neu.coe.csye7200.model.{AlphaVantageModel, AlphaVantageQuery, Model, Query}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Alpha Vantage's free `GLOBAL_QUOTE` endpoint -- the first genuinely live provider registered
  * here. Requires an API key (https://www.alphavantage.co/support/#api-key, free to obtain) in
  * the `ALPHAVANTAGE_API_KEY` environment variable; never write the key value into any committed
  * file.
  *
  * `query` is a `def`, not a `val`: the API-key check only runs when this provider is actually
  * selected as the active engine and its query is built (in `HedgeFund.startup`), not merely when
  * it's referenced from `ProviderRegistry.providers` -- otherwise running with a different engine
  * selected would fail just from the registry existing.
  *
  * @author robinhillyard
  */
object AlphaVantageProvider extends MarketDataProvider {

  private val apiKeyEnvVar = "ALPHAVANTAGE_API_KEY"

  def protocol: String = "json:AV"

  def query: Query = {
    val apiKey = sys.env.getOrElse(apiKeyEnvVar, "")
    require(apiKey.nonEmpty, s"Set the $apiKeyEnvVar environment variable to use AlphaVantageProvider")
    AlphaVantageQuery(apiKey)
  }

  val model: Model = new AlphaVantageModel

  def parserBehavior(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = JsonAlphaVantageParser(blackboard)

  // Alpha Vantage's free tier allows 1 request/second. This interval is measured between
  // firing successive ExternalLookup messages, not between the actual HTTP requests hitting
  // the wire -- a real run showed the first request's connection setup (TLS handshake etc.)
  // can eat several hundred ms, shrinking the effective gap between the first two requests
  // well below the configured interval. 2s leaves enough margin to absorb that.
  override def requestInterval: FiniteDuration = 2.seconds

  override def decodePrice(symbol: String, entity: HttpEntity.Strict): Try[Double] =
    JsonAlphaVantageParser.decode(entity) match {
      case Right(response) =>
        (model.getKey("price"), response.get("Global Quote")) match {
          case (Some(priceKey), Some(quote)) =>
            quote.get(priceKey) match {
              case Some(s) => Try(s.toDouble)
              case None => Failure(new Exception(s"no '$priceKey' field for $symbol"))
            }
          case _ => Failure(new Exception(s"could not decode price for $symbol"))
        }
      case Left(err) => Failure(new Exception(err.errorMessage))
    }
}

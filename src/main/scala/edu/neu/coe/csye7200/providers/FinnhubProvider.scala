package edu.neu.coe.csye7200.providers

import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model.HttpEntity
import edu.neu.coe.csye7200.actors.{ContentMessage, HedgeFundCommand, JsonFinnhubParser}
import edu.neu.coe.csye7200.model.{FinnhubModel, FinnhubQuery, Model, Query}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Finnhub's free `/quote` endpoint (https://finnhub.io/docs/api/quote) -- registered because
  * Alpha Vantage's free tier caps out at 25 requests/day, which a single day of testing can
  * exhaust. Finnhub's free tier is far more generous (60 requests/minute, no daily cap) and its
  * response is a single flat object of numeric fields, decoded directly as `Map[String, Double]`
  * (see `JsonFinnhubParser`).
  *
  * Requires an API key (free signup at https://finnhub.io/register) in the `FINNHUB_API_KEY`
  * environment variable; never write the key value into any committed file. As with
  * `AlphaVantageProvider`, `query` is a `def`, not a `val`, so the key check only runs when this
  * provider is actually selected as the active engine.
  *
  * @author robinhillyard
  */
object FinnhubProvider extends MarketDataProvider {

  private val apiKeyEnvVar = "FINNHUB_API_KEY"

  def protocol: String = "json:FH"

  def query: Query = {
    val apiKey = sys.env.getOrElse(apiKeyEnvVar, "")
    require(apiKey.nonEmpty, s"Set the $apiKeyEnvVar environment variable to use FinnhubProvider")
    FinnhubQuery(apiKey)
  }

  val model: Model = new FinnhubModel

  def parserBehavior(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = JsonFinnhubParser(blackboard)

  // Finnhub's free tier allows 60 requests/minute; 1 request/second stays comfortably under that.
  override def requestInterval: FiniteDuration = 1.second

  override def decodePrice(symbol: String, entity: HttpEntity.Strict): Try[Double] =
    JsonFinnhubParser.decode(entity) match {
      case Right(quote) =>
        model.getKey("price") match {
          case Some(priceKey) =>
            quote.get(priceKey) match {
              case Some(price) => Success(price)
              case None => Failure(new Exception(s"no '$priceKey' field for $symbol"))
            }
          case None => Failure(new Exception("'price' key undefined in model"))
        }
      case Left(err) => Failure(new Exception(err.errorMessage))
    }
}

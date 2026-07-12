package edu.neu.coe.csye7200.cache

import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import edu.neu.coe.csye7200.providers.MarketDataProvider

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

sealed trait PriceCacheCommand

case class GetPrice(replyTo: ActorRef[PriceResponse]) extends PriceCacheCommand

private[cache] case object Expire extends PriceCacheCommand

private[cache] case class FetchCompleted(result: Try[Double]) extends PriceCacheCommand

case class PriceResponse(symbol: String, price: Try[Double])

/**
  * One actor per symbol, caching its most recently fetched price until `ttl` elapses, at which
  * point the cached value is evicted (not eagerly refetched) so the next `GetPrice` triggers a
  * fresh fetch. Fetches go directly through the configured `MarketDataProvider` (`query` + `Http`
  * + `decodePrice`) rather than through `HedgeFundBlackboard`/`EntityParser`, which is shaped for
  * a different problem (broadcast N lookups at startup, no per-request correlation back to a
  * specific asker).
  *
  * @author robinhillyard
  */
object PriceCacheActor {

  def apply(symbol: String, ttl: FiniteDuration, provider: MarketDataProvider): Behavior[PriceCacheCommand] =
    Behaviors.setup { context =>
      implicit val system: akka.actor.typed.ActorSystem[_] = context.system
      implicit val ec: scala.concurrent.ExecutionContext = context.executionContext
      withFetcher(symbol, ttl, () => fetchPrice(symbol, provider))
    }

  /**
    * Test seam: lets a spec substitute a fake, deterministic fetch (matching `ex-cache`'s own
    * `MockStock`) instead of a real Alpha Vantage HTTP call.
    */
  private[cache] def withFetcher(symbol: String, ttl: FiniteDuration, fetchPrice: () => Future[Double]): Behavior[PriceCacheCommand] =
    Behaviors.setup { context =>
      Behaviors.withTimers { timers =>
        empty(symbol, ttl, fetchPrice, context, timers)
      }
    }

  private def empty(symbol: String, ttl: FiniteDuration, fetchPrice: () => Future[Double], context: ActorContext[PriceCacheCommand], timers: TimerScheduler[PriceCacheCommand]): Behavior[PriceCacheCommand] =
    Behaviors.receiveMessage {
      case GetPrice(replyTo) =>
        context.pipeToSelf(fetchPrice()) { result => FetchCompleted(result) }
        fetching(symbol, ttl, Seq(replyTo), fetchPrice, context, timers)
      case Expire | FetchCompleted(_) =>
        Behaviors.same
    }

  private def fetching(symbol: String, ttl: FiniteDuration, waiters: Seq[ActorRef[PriceResponse]], fetchPrice: () => Future[Double], context: ActorContext[PriceCacheCommand], timers: TimerScheduler[PriceCacheCommand]): Behavior[PriceCacheCommand] =
    Behaviors.receiveMessage {
      case GetPrice(replyTo) =>
        // A fetch is already in flight for this symbol -- just join the waiters instead of
        // starting a second HTTP call. This is what prevents a cache stampede.
        fetching(symbol, ttl, waiters :+ replyTo, fetchPrice, context, timers)
      case FetchCompleted(result) =>
        waiters.foreach(_ ! PriceResponse(symbol, result))
        result match {
          case Success(price) =>
            timers.startSingleTimer(Expire, ttl)
            cached(symbol, ttl, price, fetchPrice, context, timers)
          case Failure(_) =>
            empty(symbol, ttl, fetchPrice, context, timers)
        }
      case Expire =>
        Behaviors.same
    }

  private def cached(symbol: String, ttl: FiniteDuration, price: Double, fetchPrice: () => Future[Double], context: ActorContext[PriceCacheCommand], timers: TimerScheduler[PriceCacheCommand]): Behavior[PriceCacheCommand] =
    Behaviors.receiveMessage {
      case GetPrice(replyTo) =>
        replyTo ! PriceResponse(symbol, Success(price))
        Behaviors.same
      case Expire =>
        // Evict only -- no eager refetch. The next GetPrice (if any) triggers a fresh fetch.
        empty(symbol, ttl, fetchPrice, context, timers)
      case FetchCompleted(_) =>
        Behaviors.same
    }

  private def fetchPrice(symbol: String, provider: MarketDataProvider)(implicit system: akka.actor.typed.ActorSystem[_], ec: scala.concurrent.ExecutionContext): Future[Double] =
    for {
      query <- Future.fromTry(Try(provider.query))
      response <- Http(system.toClassic).singleRequest(HttpRequest(uri = query.createQuery(List(symbol))))
      strict <- response.entity.toStrict(10.seconds)
      price <- Future.fromTry(provider.decodePrice(symbol, strict))
    } yield price
}

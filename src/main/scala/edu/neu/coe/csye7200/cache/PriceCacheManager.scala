package edu.neu.coe.csye7200.cache

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

case class GetPriceFor(symbol: String, replyTo: ActorRef[PriceResponse])

/**
  * Routes `GetPriceFor` requests to a per-symbol `PriceCacheActor`, spawning one lazily on the
  * first request for a symbol not yet seen. Pure routing -- no HTTP or timer logic of its own.
  *
  * @author robinhillyard
  */
object PriceCacheManager {

  def apply(ttl: FiniteDuration): Behavior[GetPriceFor] = Behaviors.setup { context =>
    def active(children: Map[String, ActorRef[PriceCacheCommand]]): Behavior[GetPriceFor] =
      Behaviors.receiveMessage {
        case GetPriceFor(symbol, replyTo) =>
          children.get(symbol) match {
            case Some(child) =>
              child ! GetPrice(replyTo)
              Behaviors.same
            case None =>
              val child = context.spawn(PriceCacheActor(symbol, ttl), s"price-$symbol")
              child ! GetPrice(replyTo)
              active(children + (symbol -> child))
          }
      }

    active(Map.empty)
  }

  /**
    * The `Future`-returning bridge between the actor-based cache and plain, actor-agnostic
    * callers such as `Position.value`/`Portfolio.value` -- those take this as a `String =>
    * Future[Double]` function parameter rather than depending on `ActorRef`/`ask` themselves.
    */
  def getPrice(manager: ActorRef[GetPriceFor], symbol: String)(implicit scheduler: Scheduler, timeout: Timeout, ec: ExecutionContext): Future[Double] =
    manager.ask[PriceResponse](replyTo => GetPriceFor(symbol, replyTo)).flatMap(response => Future.fromTry(response.price))
}

package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

/**
  * Parses Finnhub's `/quote` response, a flat `Map[String, Double]` (e.g. `{"c": 261.74, "pc":
  * 262.49, ...}`) -- simpler than Alpha Vantage's nested "Global Quote" shape, but with one real
  * limitation: the response never echoes back the symbol it was quoting. `JsonAlphaVantageParser`
  * can attribute a response to a symbol purely from its payload; this one can't, which matters for
  * the legacy broadcast-style `HedgeFundBlackboard`/`EntityParser` pipeline (no per-request
  * correlation id there either). That pipeline is no longer invoked live by `hedgeFundApp` anyway
  * -- `cache.PriceCacheActor` calls `FinnhubProvider.decodePrice` directly instead, already
  * knowing its symbol from closure state. This parser exists mainly so `FinnhubProvider` satisfies
  * `MarketDataProvider`'s full interface, and so `decode` has fixture-driven test coverage.
  *
  * @author robinhillyard
  */
object JsonFinnhubParser {

  import edu.neu.coe.csye7200.http.JsonUnmarshalling
  import edu.neu.coe.csye7200.http.JsonUnmarshalling.Deserialized
  import akka.http.scaladsl.model.HttpEntity

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = Behaviors.setup { context =>
    Behaviors.receiveMessage {
      case ContentMessage(entity) =>
        context.log.debug("JsonFinnhubParser received ContentMessage")
        decode(entity) match {
          case Right(_) =>
            context.log.warn("Finnhub's /quote response does not identify the symbol it quotes, " +
              "so it cannot be routed via this broadcast-style pipeline; ignoring")
          case Left(message) => context.log.warn(message.toString)
        }
        Behaviors.same
    }
  }

  object MyJsonProtocol extends spray.json.DefaultJsonProtocol with spray.json.NullOptions {
  }

  import MyJsonProtocol._

  type Response = Map[String, Double]

  def decode(entity: HttpEntity.Strict): Deserialized[Response] = JsonUnmarshalling.decode[Response](entity.data)

}

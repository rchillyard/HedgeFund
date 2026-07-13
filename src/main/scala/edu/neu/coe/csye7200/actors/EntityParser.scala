package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.providers.ProviderRegistry

/**
  * @author robinhillyard
  */
object EntityParser {

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[EntityMessage] = Behaviors.setup { context =>
    // Stock-quote providers are registered once in ProviderRegistry; option-chain parsing
    // (out of scope for the provider plugin architecture -- no candidate provider offers a
    // free option-chain tier) stays wired here directly, as it always has been.
    val parsers: Map[String, ActorRef[ContentMessage]] =
      ProviderRegistry.providers.values.map { p =>
        p.protocol -> context.spawn(p.parserBehavior(blackboard), s"parser-${p.protocol.replace(':', '-')}")
      }.toMap + ("json:GO" -> context.spawn(JsonGoogleOptionParser(blackboard), "JsonGoogleOptionParser"))

    Behaviors.receiveMessage {
      case EntityMessage(protocol, entity) =>
        context.log.debug("EntityMessage received: protocol: {}", protocol)
        parsers.get(protocol) match {
          case Some(actorRef) => actorRef ! ContentMessage(entity)
          case None => context.log.warn("no parser for: {}", protocol)
        }
        Behaviors.same
    }
  }
}

case class ContentMessage(content: akka.http.scaladsl.model.HttpEntity.Strict)

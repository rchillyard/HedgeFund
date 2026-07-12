package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

/**
  * @author robinhillyard
  */
object EntityParser {

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[EntityMessage] = Behaviors.setup { context =>
    val parsers: Map[String, ActorRef[ContentMessage]] = Map(
      "json:YQL" -> context.spawn(JsonYQLParser(blackboard), "JsonParserYQL"),
      "json:GF" -> context.spawn(JsonGoogleParser(blackboard), "JsonGoogleParser"),
      "json:GO" -> context.spawn(JsonGoogleOptionParser(blackboard), "JsonGoogleOptionParser"))

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

package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import akka.http.scaladsl.model._

/**
  * @author robinhillyard
  */
object HttpReader {

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[HttpReaderCommand] = Behaviors.setup { context =>
    val entityParser: ActorRef[EntityMessage] = context.spawn(EntityParser(blackboard), "EntityParser")

    def processResponse(entity: HttpEntity.Strict, headers: Seq[HttpHeader], protocol: String): Unit = {
      context.log.debug("response headers: {}; entity: {}", headers, entity)
      entityParser ! EntityMessage(protocol, entity)
    }

    Behaviors.receiveMessage {
      case HttpResult(queryProtocol, request, HttpResponse(status, headers, entity: HttpEntity.Strict, protocol)) =>
        context.log.info("request sent: {}; protocol: {}; response status: {}", request, protocol, status)
        if (status.isSuccess)
          processResponse(entity, headers, queryProtocol)
        else
          context.log.error("HTTP transaction error: {}", status.reason())
        Behaviors.same
    }
  }
}

// TODO add headers
// CONSIDER move into Blackboard
case class EntityMessage(protocol: String, entity: HttpEntity.Strict)

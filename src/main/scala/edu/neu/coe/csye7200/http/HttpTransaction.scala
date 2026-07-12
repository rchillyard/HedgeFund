package edu.neu.coe.csye7200.http

import akka.actor.ActorRef
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import edu.neu.coe.csye7200.actors.HttpResult

import scala.concurrent._
import scala.concurrent.duration._

/**
  * CONSIDER making this an Actor
  *
  * @author robinhillyard
  */
case class HttpTransaction(queryProtocol: String, request: HttpRequest, actor: ActorRef)(implicit system: akka.actor.ActorSystem, ec: ExecutionContext) {

  import akka.pattern.pipe

  val strictEntityTimeout: FiniteDuration = 30.seconds

  val response: Future[HttpResponse] =
    Http(system).singleRequest(request).flatMap { r =>
      r.entity.toStrict(strictEntityTimeout).map(strict => r.withEntity(strict))
    }

  response map { x => HttpResult(queryProtocol, request, x) } pipeTo actor

}


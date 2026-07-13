package edu.neu.coe.csye7200.http

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import edu.neu.coe.csye7200.actors.{HttpReaderCommand, HttpResult}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * CONSIDER making this an Actor
  *
  * @author robinhillyard
  */
case class HttpTransaction(queryProtocol: String, request: HttpRequest, actor: ActorRef[HttpReaderCommand])(implicit system: akka.actor.typed.ActorSystem[_], ec: ExecutionContext) {

  val strictEntityTimeout: FiniteDuration = 30.seconds

  val response: Future[HttpResponse] =
    Http(system.toClassic).singleRequest(request).flatMap { r =>
      r.entity.toStrict(strictEntityTimeout).map(strict => r.withEntity(strict))
    }

  response.onComplete {
    case Success(r) => actor ! HttpResult(queryProtocol, request, r)
    case Failure(ex) => system.log.warn("HttpTransaction failed for protocol {}: {}", queryProtocol, ex.getLocalizedMessage)
  }

}

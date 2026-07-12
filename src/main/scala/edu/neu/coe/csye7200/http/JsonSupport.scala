package edu.neu.coe.csye7200.http

import akka.util.ByteString
import spray.json._

import scala.util.{Failure, Success, Try}

/**
  * Replacement for spray.httpx.unmarshalling's Deserialized/MalformedContent, adapted to decode
  * directly from a strict entity's bytes rather than going through Akka HTTP's Future-based Unmarshal
  * (unnecessary here since every entity this app decodes is already fully in memory: either a local
  * test fixture or a network response drained to HttpEntity.Strict by HttpTransaction).
  *
  * @author robinhillyard
  */
case class MalformedContent(errorMessage: String)

object JsonUnmarshalling {

  type Deserialized[T] = Either[MalformedContent, T]

  def decode[T: JsonReader](data: ByteString): Deserialized[T] = decode(data.utf8String)

  def decode[T: JsonReader](json: String): Deserialized[T] =
    Try(json.parseJson.convertTo[T]) match {
      case Success(t) => Right(t)
      case Failure(e) => Left(MalformedContent(e.getLocalizedMessage))
    }
}

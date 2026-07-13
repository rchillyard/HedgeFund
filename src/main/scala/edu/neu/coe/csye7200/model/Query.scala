package edu.neu.coe.csye7200.model

import akka.http.scaladsl.model.Uri

/**
  * @author robinhillyard
  */
trait Query {
  def createQuery(symbols: List[String]): Uri

  def getProtocol: String
}
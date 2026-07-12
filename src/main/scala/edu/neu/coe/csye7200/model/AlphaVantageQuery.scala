package edu.neu.coe.csye7200.model

import akka.http.scaladsl.model.Uri
import edu.neu.coe.csye7200.http.UriGet

/**
  * Alpha Vantage's free `GLOBAL_QUOTE` endpoint (https://www.alphavantage.co/documentation/#latestprice) --
  * unlike the old YQL/Google Finance batch endpoints, it accepts exactly one symbol per call, so
  * `createQuery` only ever looks at `symbols.head`.
  *
  * @author robinhillyard
  */
case class AlphaVantageQuery(apiKey: String) extends Query {
  val uriGet = new UriGet()

  def createQuery(symbols: List[String]): Uri = {
    val queryParams = Map("function" -> "GLOBAL_QUOTE", "symbol" -> symbols.head, "apikey" -> apiKey)
    uriGet.get(AlphaVantageQuery.server, AlphaVantageQuery.path, queryParams)
  }

  def getProtocol = "json:AV"
}

object AlphaVantageQuery {
  val server = "www.alphavantage.co"
  val path = "/query"
}

class AlphaVantageModel extends Model {
  def isOption = false

  def getKey(query: String): Option[String] = query match {
    case "name" => Some("AlphaVantage")
    case "symbol" => Some("01. symbol")
    case "price" => Some("05. price")
    case _ => None
  }
}

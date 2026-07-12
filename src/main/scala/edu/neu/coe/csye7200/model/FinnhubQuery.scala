package edu.neu.coe.csye7200.model

import akka.http.scaladsl.model.Uri
import edu.neu.coe.csye7200.http.UriGet

/**
  * Finnhub's free `/quote` endpoint (https://finnhub.io/docs/api/quote) -- like Alpha Vantage's
  * `GLOBAL_QUOTE`, one symbol per call. Its response is a flat object of numeric fields (current
  * price, change, previous close, etc.) with no per-provider nested wrapper and, notably, no
  * field echoing back the symbol that was quoted.
  *
  * @author robinhillyard
  */
case class FinnhubQuery(apiKey: String) extends Query {
  val uriGet = new UriGet()

  def createQuery(symbols: List[String]): Uri = {
    val queryParams = Map("symbol" -> symbols.head, "token" -> apiKey)
    uriGet.get(FinnhubQuery.server, FinnhubQuery.path, queryParams)
  }

  def getProtocol = "json:FH"
}

object FinnhubQuery {
  val server = "finnhub.io"
  val path = "/api/v1/quote"
}

class FinnhubModel extends Model {
  def isOption = false

  def getKey(query: String): Option[String] = query match {
    case "name" => Some("Finnhub")
    case "price" => Some("c")
    case "previousClose" => Some("pc")
    case _ => None
  }
}

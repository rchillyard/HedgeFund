package edu.neu.coe.csye7200.portfolio

import scala.concurrent.{ExecutionContext, Future}

/**
  * CONSIDER moving this into model package
  *
  * @author robinhillyard
  */
case class Portfolio(name: String, positions: Seq[Position]):
  /**
    * @param getPrice a lookup for a symbol's current price -- in practice
    *                 `PriceCacheManager.getPrice(manager, _)` partially applied, but this type
    *                 keeps Portfolio/Position free of any dependency on actors or `ask`.
    */
  def value(getPrice: String => Future[Double])(implicit ec: ExecutionContext): Future[Double] =
    Future.sequence(positions.map(_.value(getPrice))).map(_.sum)

/**
  * Represents a position in a portfolio, which includes details about the financial instrument,
  * the quantity held, and a list of associated contracts.
  *
  * @param symbol    The symbol identifying the financial instrument, such as a stock or option ticker.
  * @param quantity  The number of units held for the specified symbol.
  * @param contracts A sequence of associated contracts linked to this position.
  */
case class Position(symbol: String, quantity: Int, contracts: Seq[Contract]):
  def value(getPrice: String => Future[Double])(implicit ec: ExecutionContext): Future[Double] =
    getPrice(symbol).map(_ * quantity)

/**
  * Represents a financial contract within a trading or investment system.
  *
  * A contract typically encompasses agreements or instruments associated with
  * specific financial transactions, such as options, futures, or other derivative products.
  *
  * @param id A unique identifier for the contract, often used to differentiate
  *           individual contracts within a collection or system.
  */
case class Contract(id: String)

/**
  * PortfolioParser is an object designed to decode JSON strings into instances of the `Portfolio` case class.
  * It utilizes the Spray JSON library to facilitate the conversion process.
  *
  * The JSON structure needs to correspond to the structure of the `Portfolio`, `Position`,
  * and `Contract` case classes, which include fields such as `name`, `positions`, `symbol`,
  * `quantity`, and `contracts`.
  *
  * The object imports and defines the necessary JSON protocols under the `MyJsonProtocol` object
  * to enable deserialization of JSON into these case classes.
  *
  * Methods:
  * - `decode`: Accepts a JSON string as input and parses it into a `Portfolio` instance.
  * Throws an exception if the provided JSON doesn't match the expected structure.
  */
object PortfolioParser {

  import spray.json.{DefaultJsonProtocol, _}

  object MyJsonProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val contractFormat: RootJsonFormat[Contract] = jsonFormat1(Contract)
    implicit val positionFormat: RootJsonFormat[Position] = jsonFormat3(Position)
    implicit val portfolioFormat: RootJsonFormat[Portfolio] = jsonFormat2(Portfolio)
  }

  import MyJsonProtocol._

  def decode(json: String): Portfolio =
    json.parseJson.convertTo[Portfolio]
}

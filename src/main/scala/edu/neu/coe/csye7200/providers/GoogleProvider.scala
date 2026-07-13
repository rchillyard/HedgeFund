package edu.neu.coe.csye7200.providers

import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.actors.{ContentMessage, HedgeFundCommand, JsonGoogleParser}
import edu.neu.coe.csye7200.model.{GoogleModel, GoogleQuery, Model, Query}

/**
  * The old Google Finance JSON endpoint, removed years ago — permanently dead, kept registered
  * only because JsonGoogleParserSpec exercises its parsing code against a local fixture (no
  * network) and the parsing logic itself (including its "//"-prefix anti-scraping workaround)
  * remains a useful teaching example.
  *
  * @author robinhillyard
  */
object GoogleProvider extends MarketDataProvider {

  val query: Query = GoogleQuery("NASDAQ")

  val model: Model = new GoogleModel

  def protocol: String = query.getProtocol

  def parserBehavior(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = JsonGoogleParser(blackboard)
}

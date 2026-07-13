package edu.neu.coe.csye7200.providers

import akka.actor.typed.{ActorRef, Behavior}
import edu.neu.coe.csye7200.actors.{ContentMessage, HedgeFundCommand, JsonYQLParser}
import edu.neu.coe.csye7200.model.{Model, Query, YQLModel, YQLQuery}

/**
  * Yahoo's YQL (Yahoo Query Language) finance-quotes endpoint, discontinued ~2019 — permanently
  * dead, kept registered only because JsonYQLParserSpec exercises its parsing code against a
  * local fixture (no network) and the parsing logic itself remains a useful teaching example.
  *
  * @author robinhillyard
  */
object YQLProvider extends MarketDataProvider {

  val query: Query = YQLQuery("json", diagnostics = false)

  val model: Model = new YQLModel

  def protocol: String = query.getProtocol

  def parserBehavior(blackboard: ActorRef[HedgeFundCommand]): Behavior[ContentMessage] = JsonYQLParser(blackboard)
}

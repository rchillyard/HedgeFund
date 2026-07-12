package edu.neu.coe.csye7200.actors

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

import scala.collection.mutable

/**
  * @author robinhillyard
  */
object MarketData {

  def apply(blackboard: ActorRef[HedgeFundCommand]): Behavior[MarketDataCommand] = Behaviors.setup { context =>

    /**
      * see definition of get(String)
      */
    val instruments: mutable.Map[String, Map[String, String]] = mutable.Map[String, Map[String, String]]()

    Behaviors.receiveMessage {
      case KnowledgeUpdate(model, identifier, update) =>
        context.log.debug("update to identifier: {}", identifier)
        instruments.put(identifier, update)
        // for a stock, we don't need additional attributes
        blackboard ! Confirmation(identifier, model, Map())
        Behaviors.same

      // CONSIDER allowing key to be null in which case all attributes returned
      // Or allow key to be a list and always return a map of values
      case SymbolQuery(identifier, keys, replyTo) =>
        println(s"symbol query received re: identifier: $identifier and keys $keys")
        context.log.debug("symbol query received re: identifier: {} and keys {}", identifier, keys)
        val attributes: List[Option[(String, String)]] = instruments.get(identifier) match {
          case Some(a) => keys map { k =>
            a.get(k) match {
              case Some(v) => Some(k -> v)
              case None => None
            }
          }
          case None => List()
        }
        val x = attributes.flatten
        val y = x.toMap
        context.log.debug(s"creating QueryResponse: $identifier $y")
        replyTo ! QueryResponse(identifier, y)
        Behaviors.same

      case OptionQuery(key, value, replyTo) =>
        context.log.debug("option query received re: key: {} and value {}", key, value)
        val optInstr = instruments find { case (_, v) => v.get(key) match {
          case Some(`value`) => true;
          case _ => false
        }
        }
        optInstr match {
          case Some((x, m)) => replyTo ! QueryResponse(x, m)
          case _ => context.log.warn("no match found for key: {}, value: {}", key, value); replyTo ! QueryResponse(null, null)
        }
        Behaviors.same
    }
  }

  /**
    * The key to the instruments collection is an "identifier".
    * In the case of stocks and similar instruments with a (ticker) symbol (or CUSIP),
    * then identifier is the symbol.
    * In the case of options, the identifier is option id.
    * //   * @param key the key (see above)
    * //   * @return the instruments value corresponding to key
    */
  //  private def get(key: String) = instruments.get(key)
}

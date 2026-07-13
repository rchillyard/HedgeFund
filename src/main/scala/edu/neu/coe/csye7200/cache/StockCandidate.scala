package edu.neu.coe.csye7200.cache

import edu.neu.coe.csye7200.rules.Candidate

/**
  * A `Candidate` representing a stock symbol's latest price change, evaluated against the
  * `buy`/`sell` rules in `stockRules.txt` via the existing `Predicate`/`SimpleRule` engine --
  * the same mechanism `OptionAnalyzer` already uses for option rules, just with a different
  * `Candidate` shape (see `rules/Candidate.scala`).
  *
  * @author robinhillyard
  */
case class StockCandidate(symbol: String, price: Double, previousPrice: Double) extends Candidate {
  def identifier: String = symbol

  private val details: Map[String, Any] = Map("price" -> price, "previousPrice" -> previousPrice, "change" -> (price - previousPrice))

  def apply(s: String): Option[Any] = details.get(s)

  // Nothing in this design merges extra properties into a StockCandidate (unlike OptionCandidate,
  // which folds in properties.txt lookups before rule evaluation), so this is a no-op.
  def ++(m: Map[String, Any]): Candidate = this
}

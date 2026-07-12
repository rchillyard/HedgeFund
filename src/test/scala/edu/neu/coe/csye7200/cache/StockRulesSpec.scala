package edu.neu.coe.csye7200.cache

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.util.Success

/**
  * Exercises the real `stockRules.txt` on the classpath (`buy="change < -2"`, `sell="change >
  * 2"`), the same way `OptionAnalyzerSpec` exercises the real `rules.txt` rather than a
  * test-only fixture.
  */
class StockRulesSpec extends AnyWordSpecLike with Matchers {

  private val log = LoggerFactory.getLogger(getClass)

  "StockRules.getRules" should {
    "read buy and sell rules from stockRules.txt" in {
      val rules = StockRules.getRules(log)
      rules.keySet shouldBe Set("buy", "sell")
    }

    "fire the buy rule when the price has dropped enough" in {
      val rules = StockRules.getRules(log)
      val dropped = StockCandidate("AAPL", price = 95.0, previousPrice = 100.0) // change = -5
      rules("buy")(dropped) shouldBe Success(true)
      rules("sell")(dropped) shouldBe Success(false)
    }

    "fire the sell rule when the price has risen enough" in {
      val rules = StockRules.getRules(log)
      val risen = StockCandidate("AAPL", price = 105.0, previousPrice = 100.0) // change = +5
      rules("sell")(risen) shouldBe Success(true)
      rules("buy")(risen) shouldBe Success(false)
    }

    "fire neither rule for a small price change" in {
      val rules = StockRules.getRules(log)
      val steady = StockCandidate("AAPL", price = 100.5, previousPrice = 100.0) // change = +0.5
      rules("buy")(steady) shouldBe Success(false)
      rules("sell")(steady) shouldBe Success(false)
    }
  }
}

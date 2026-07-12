package edu.neu.coe.csye7200.cache

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class StockCandidateSpec extends AnyWordSpecLike with Matchers {

  "StockCandidate" should {
    "expose price, previousPrice, and change" in {
      val candidate = StockCandidate("MSFT", price = 110.0, previousPrice = 100.0)
      candidate("price") shouldBe Some(110.0)
      candidate("previousPrice") shouldBe Some(100.0)
      candidate("change") shouldBe Some(10.0)
    }

    "use the symbol as its identifier" in {
      StockCandidate("MSFT", 1.0, 1.0).identifier shouldBe "MSFT"
    }

    "return None for an unknown key" in {
      StockCandidate("MSFT", 1.0, 1.0)("unknown") shouldBe None
    }

    "leave itself unchanged when ++ is applied (no extra properties supported)" in {
      val candidate = StockCandidate("MSFT", 110.0, 100.0)
      (candidate ++ Map("extra" -> "value")) shouldBe candidate
    }
  }
}

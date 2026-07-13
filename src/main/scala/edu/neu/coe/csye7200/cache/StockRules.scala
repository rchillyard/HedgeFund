package edu.neu.coe.csye7200.cache

import com.typesafe.config.ConfigFactory
import edu.neu.coe.csye7200.rules.{Predicate, SimpleRule}
import org.slf4j.Logger

import scala.util.Try

/**
  * Reads the `buy`/`sell` rules from `stockRules.txt`, mirroring
  * `OptionAnalyzer.getRules`'s shape for the option-analysis rules in `rules.txt`, but for
  * `StockCandidate` instead of `OptionCandidate`.
  *
  * @author robinhillyard
  */
object StockRules {

  def getRules(log: Logger): Map[String, Predicate] = {
    val sRules = "stockRules.txt"
    val sSysRules = s"/$sRules"
    val co = Try(ConfigFactory.parseURL(getClass.getResource(sSysRules))).toOption

    co match {
      case Some(config) =>
        List("buy", "sell").map { k =>
          val r = config.getString(k)
          log.info(s"rule: $k -> $r")
          k -> SimpleRule(r)
        }.toMap
      case None =>
        log.warn(s"unable to read stock rules configuration: $sSysRules")
        Map()
    }
  }
}

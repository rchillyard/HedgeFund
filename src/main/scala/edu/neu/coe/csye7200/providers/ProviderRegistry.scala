package edu.neu.coe.csye7200.providers

/**
  * The compile-time registry of all known market-data providers, keyed by the name used in
  * `application.conf`'s `engine` setting. To add a new provider: implement `MarketDataProvider`
  * (see `AlphaVantageProvider` for a worked example) and add one entry here -- no other file
  * needs to change (`EntityParser` and `HedgeFund.startup` both derive from this map).
  *
  * @author robinhillyard
  */
object ProviderRegistry {
  val providers: Map[String, MarketDataProvider] = Map(
    "YQL" -> YQLProvider,
    "Google" -> GoogleProvider,
    "AlphaVantage" -> AlphaVantageProvider
  )
}

package edu.neu.coe.csye7200

import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import com.typesafe.config.{Config, ConfigFactory}
import edu.neu.coe.csye7200.actors.{ExternalLookup, HedgeFundBlackboard, HedgeFundCommand, PortfolioUpdate}
import edu.neu.coe.csye7200.model.GoogleOptionQuery
import edu.neu.coe.csye7200.portfolio.{Portfolio, PortfolioParser}
import edu.neu.coe.csye7200.providers.ProviderRegistry
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.io.Source
import scala.language.implicitConversions
import scala.util._

/**
  * @author robinhillyard
  */
object HedgeFund {

  def startup(config: Config)(implicit system: ActorSystem[HedgeFundCommand]): Try[ActorRef[HedgeFundCommand]] = {
    ProviderRegistry.providers.get(config.getString("engine")) match {
      case Some(provider) =>
        // provider.query is deferred (a def, not a val) specifically so a provider needing
        // an API key only fails here, when actually selected -- not merely by being listed
        // in ProviderRegistry. Try(...) turns a missing key into a clean Failure rather than
        // an uncaught exception, matching this method's existing error-handling style.
        Try(provider.query) match {
          case Success(query) =>
            getPortfolio(config) match {
              case Some(portfolio) =>
                val blackboard: ActorRef[HedgeFundCommand] = system
                val symbols = getSymbols(config, portfolio)
                // One request per symbol, uniformly for every provider: Alpha Vantage's free
                // GLOBAL_QUOTE endpoint accepts exactly one symbol per call, so this can no
                // longer rely on YQL/Google's old batch-all-symbols-into-one-URI style. Staggered
                // by provider.requestInterval (zero for providers with no meaningful rate limit)
                // -- confirmed necessary by a real run against Alpha Vantage's free tier, which
                // otherwise rate-limits all but the first of several near-simultaneous requests.
                symbols.zipWithIndex foreach {
                  case (s, i) =>
                    if (i > 0) Thread.sleep(provider.requestInterval.toMillis)
                    blackboard ! ExternalLookup(provider.protocol, query.createQuery(List(s)))
                }
                val optionEngine = new GoogleOptionQuery
                symbols foreach {
                  s => blackboard ! ExternalLookup(optionEngine.getProtocol, optionEngine.createQuery(List(s)))
                }
                blackboard ! PortfolioUpdate(portfolio)
                Success(blackboard)

              case None => Failure(new Exception(s"configuration has errors--see logs"))
            }
          case Failure(x) => Failure(x)
        }

      case _ => Failure(new Exception("initialization engine not defined"))
    }
  }

  import scala.language.postfixOps

  def getSymbols(config: Config, portfolio: Portfolio): List[String] = {
    // TODO add in the symbols from the portfolio
    config.getString("symbols") split "\\," toList
  }

  def getPortfolio(config: Config): Option[Portfolio] = {
    val filename = config.getString("portfolio")
    implicit val clazz: Class[_] = getClass
    val json = for (s <- getSource(filename)) yield s.mkString
    json map PortfolioParser.decode
  }

  /**
    * Get a Source corresponding to filename and, optionally, clazz.
    *
    * NOTE: we try two different ways of getting the file:
    * (1) where file is a pure filename relative to the filing system;
    * (2) where file is the name of a resource relative to the given class (or current class if clazz == null)
    *
    * NOTE: that all of this is going away in 2.12 because there is a fromResource method in Source there.
    *
    * @param filename the filename to be used as the Source
    * @param clazz    in the case that filename cannot be opened, we will use filename as the name of a resource
    *                 relative to the given class.
    * @return an optional Source
    */
  def getSource(filename: String)(implicit clazz: Class[_] = null): Option[Source] = {
    def getSource(clazz: Class[_]): Source = Source.fromURL(clazz.getResource(filename))

    val getSourceOptional: Option[Class[_]] => Option[Source] = _ map getSource
    Try(Source.fromFile(filename)).recoverWith { case e: Throwable => logger.warn(e.getLocalizedMessage); Failure(e) }.toOption orElse
      getSourceOptional(Option(clazz))
  }

  val logger: Logger = LoggerFactory.getLogger(getClass)

}

@main def hedgeFundApp(): Unit = {
  val config = ConfigFactory.load()
  println(s"""${config.getString("name")}, ${config.getString("appVersion")}""")
  implicit val system: ActorSystem[HedgeFundCommand] = ActorSystem(HedgeFundBlackboard(), "HedgeFund")
  HedgeFund.startup(config) match {
    case Success(_) =>
      // startup only fires the ExternalLookup pipeline off asynchronously (tell, not ask) --
      // terminate() below begins shutdown immediately, so without a real pause here almost
      // none of the actual HTTP round trips (to whichever provider is configured) get a
      // chance to complete before the actor system tears down.
      Thread.sleep(20000)
    case Failure(x) =>
      println(s"startup failed: ${x.getMessage}")
      HedgeFund.logger.error("startup failed: {}", x.getMessage)
  }
  // Shut down Akka HTTP's connection pools deliberately before tearing down the actor
  // system -- otherwise terminate() kills the pool actors abruptly (mid keep-alive) rather
  // than closing them properly, producing "Unexpected termination of TLS actor" warnings.
  // The extra pause after this call matters: shutdownAllConnectionPools()'s Future resolves
  // once the pool stops handing out new work, which is not the same instant as the underlying
  // TLS close-notify handshake actually finishing flushing over the network to each remote
  // server -- confirmed by direct testing that omitting this pause still produced the warning.
  Try(Await.result(Http(system.toClassic).shutdownAllConnectionPools(), FiniteDuration(10, "seconds"))) match {
    case Success(_) =>
    case Failure(x) => HedgeFund.logger.warn("connection pool shutdown failed: {}", x.getMessage)
  }
  Thread.sleep(2000)
  system.terminate()
  Await.ready(system.whenTerminated, FiniteDuration(5, "seconds"))
}

package edu.neu.coe.csye7200

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import edu.neu.coe.csye7200.actors.{MarketData, SymbolQuery}
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Inside, TryValues}
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * This specification really tests much of the HedgeFund app but because it particularly deals with
  * processing data from the YQL (Yahoo Query Language) using JSON, we call it by its given name.
  */
class HedgeFundSpec extends AnyFlatSpec with Matchers with Futures with ScalaFutures with TryValues with Inside {

  behavior of "SymbolQuery"

  // Ignored as of 2026-07-12: this test depends on two external market-data APIs that are permanently
  // discontinued (Yahoo's YQL endpoint, retired ~2019; the old undocumented Google Finance JSON endpoint,
  // removed years ago), not merely flaky, so there is no live endpoint left to reach regardless of how
  // this test is written. JsonYQLParserSpec/JsonGoogleParserSpec/JsonGoogleOptionParserSpec already
  // exercise the identical EntityParser -> parser -> MarketData -> blackboard pipeline via synthetic
  // EntityMessages built from local JSON fixtures, and are the equivalent coverage this test would
  // otherwise need.
  it should "work" ignore {
    import scala.concurrent.duration._
    implicit val system: ActorSystem = ActorSystem("HedgeFund")
    implicit val timeout: Timeout = Timeout(30.seconds)
    val ay = HedgeFund.startup(ConfigFactory.load())
    ay should matchPattern { case Success(_) => }
    val qf: Future[MarketData] = (ay match {
      case Success(a) => a ? SymbolQuery("MSFT", List("name", "symbol", "price", "GF", "t", "l"))
      case Failure(x) => Future.failed(x)
    }).mapTo[MarketData]
    whenReady(qf, org.scalatest.concurrent.PatienceConfiguration.Timeout(Span(10, Seconds))) { q => println(q) }
  }

}


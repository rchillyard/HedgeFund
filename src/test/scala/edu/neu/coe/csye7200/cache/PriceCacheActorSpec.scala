package edu.neu.coe.csye7200.cache

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Success

class PriceCacheActorSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {

  val testKit: ActorTestKit = ActorTestKit()

  override def afterAll(): Unit = testKit.shutdownTestKit()

  "PriceCacheActor" should {

    "fetch once and serve subsequent requests from cache" in {
      val fetchCount = new AtomicInteger(0)

      def fetchPrice(): Future[Double] = {
        fetchCount.incrementAndGet()
        Future.successful(100.0)
      }

      val actor = testKit.spawn(PriceCacheActor.withFetcher("TEST", 10.seconds, fetchPrice))
      val probe = testKit.createTestProbe[PriceResponse]()

      actor ! GetPrice(probe.ref)
      probe.expectMessage(PriceResponse("TEST", Success(100.0)))

      actor ! GetPrice(probe.ref)
      probe.expectMessage(PriceResponse("TEST", Success(100.0)))

      fetchCount.get() shouldBe 1
    }

    "not start a second fetch while one is already in flight (no cache stampede)" in {
      val fetchCount = new AtomicInteger(0)
      val promise = scala.concurrent.Promise[Double]()

      def fetchPrice(): Future[Double] = {
        fetchCount.incrementAndGet()
        promise.future
      }

      val actor = testKit.spawn(PriceCacheActor.withFetcher("TEST2", 10.seconds, fetchPrice))
      val probe1 = testKit.createTestProbe[PriceResponse]()
      val probe2 = testKit.createTestProbe[PriceResponse]()

      actor ! GetPrice(probe1.ref)
      actor ! GetPrice(probe2.ref)
      Thread.sleep(200) // give the actor time to process both GetPrice messages before the fetch completes
      promise.success(200.0)

      probe1.expectMessage(PriceResponse("TEST2", Success(200.0)))
      probe2.expectMessage(PriceResponse("TEST2", Success(200.0)))
      fetchCount.get() shouldBe 1
    }

    "refetch after the cached value expires" in {
      val fetchCount = new AtomicInteger(0)

      def fetchPrice(): Future[Double] = {
        val n = fetchCount.incrementAndGet()
        Future.successful(n * 10.0)
      }

      val actor = testKit.spawn(PriceCacheActor.withFetcher("TEST3", 200.millis, fetchPrice))
      val probe = testKit.createTestProbe[PriceResponse]()

      actor ! GetPrice(probe.ref)
      probe.expectMessage(PriceResponse("TEST3", Success(10.0)))

      Thread.sleep(400) // past the 200ms TTL

      actor ! GetPrice(probe.ref)
      probe.expectMessage(PriceResponse("TEST3", Success(20.0)))

      fetchCount.get() shouldBe 2
    }
  }
}

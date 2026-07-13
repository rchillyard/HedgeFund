package edu.neu.coe.csye7200.rss

import java.net.URL
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, NodeSeq, XML}

case class XmlMessage(xml: Elem)

object Reader {
  def print(feed: RssFeed): Unit = println(feed.latest)
}

object AtomReader {
  def apply(): Behavior[XmlMessage] = Behaviors.setup { _ =>
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH)

    def parseAtomDate(date: String, formatter: SimpleDateFormat): Date = {
      val newDate = date.reverse.replaceFirst(":", "").reverse
      formatter.parse(newDate)
    }

    def getHtmlLink(node: NodeSeq) = {
      node
        .filter(n => (n \ "@type").text == "text/html")
        .map(n => (n \ "@href").text).head
    }

    def extract(xml: Elem): Seq[RssFeed] = {
      for (feed <- xml \\ "feed") yield {
        val items = for (item <- feed \\ "entry") yield {
          RssItem(
            (item \\ "title").text,
            getHtmlLink(item \\ "link"),
            (item \\ "summary").text,
            parseAtomDate((item \\ "published").text, dateFormatter),
            (item \\ "id").text)
        }
        AtomRssFeed(
          (feed \ "title").text,
          getHtmlLink(feed \ "link"),
          (feed \ "subtitle ").text,
          items.take(8))
      }
    }

    Behaviors.receiveMessage {
      case XmlMessage(xml) =>
        extract(xml) match {
          case head :: _ => Reader.print(head)
          case Nil =>
        }
        Behaviors.same
    }
  }
}

object XmlReader {
  def apply(): Behavior[XmlMessage] = Behaviors.setup { _ =>
    val dateFormatter = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH)

    def extract(xml: Elem): Seq[RssFeed] = {
      for (channel <- xml \\ "channel") yield {
        val items = for (item <- channel \\ "item") yield {
          RssItem(
            (item \\ "title").text,
            (item \\ "link").text,
            (item \\ "description").text,
            dateFormatter.parse((item \\ "pubDate").text),
            (item \\ "guid").text)
        }
        XmlRssFeed(
          (channel \ "title").text,
          (channel \ "link").text,
          (channel \ "description").text,
          (channel \ "language").text,
          items.take(8))
      }
    }

    Behaviors.receiveMessage {
      case XmlMessage(xml) =>
        extract(xml) match {
          case head :: _ => Reader.print(head)
          case Nil =>
        }
        Behaviors.same
    }
  }
}

case class ReadUrl(url: URL)

object RssReader {

  def apply(): Behavior[ReadUrl] = Behaviors.setup { context =>

    def read(url: URL): Unit = {
      Try(url.openConnection.getInputStream) match {
        case Success(u) =>
          val xml = XML.load(u)
          val actor: ActorRef[XmlMessage] =
            if ((xml \\ "channel").length == 0) context.spawnAnonymous(AtomReader())
            else context.spawnAnonymous(XmlReader())
          actor ! XmlMessage(xml)
        case Failure(_) =>
      }
    }

    Behaviors.receiveMessage {
      case ReadUrl(url) =>
        read(url)
        Behaviors.same
    }
  }

  def getUrls(fileName: String): Array[RssUrl] = getFileLines(fileName).map(url => RssUrl(new URL(url)))

  def getFileLines(fileName: String): Array[String] =
    scala.io.Source.fromFile(fileName).mkString.split("\n").filter(!_.startsWith("#"))
}

case class ReadSubscriptions(filename: String, replyTo: ActorRef[Seq[URL]])

object SubscriptionReader {

  def open(filename: String): Elem = XML.loadFile(filename)

  def read(xml: Elem): Seq[URL] =
    for {
      node <- xml \\ "@xmlUrl"
    } yield new URL(node.text)

  def apply(): Behavior[ReadSubscriptions] = Behaviors.receiveMessage {
    case ReadSubscriptions(filename, replyTo) =>
      replyTo ! read(open(filename))
      Behaviors.same
  }
}

@main def rssReaderApp(): Unit = {
  import akka.actor.typed.scaladsl.AskPattern._

  implicit val system: ActorSystem[ReadSubscriptions] = ActorSystem(SubscriptionReader(), "RssReader")
  implicit val timeout: Timeout = Timeout(30.seconds)
  implicit val ec: ExecutionContext = system.executionContext

  val urlsFuture: scala.concurrent.Future[Seq[URL]] = system.ask(replyTo => ReadSubscriptions("subscriptions.xml", replyTo))
  urlsFuture.foreach { urls =>
    urls.zipWithIndex.foreach { case (url, i) =>
      val rssReader = system.systemActorOf(RssReader(), s"rssReader-$i")
      rssReader ! ReadUrl(url)
    }
  }
}

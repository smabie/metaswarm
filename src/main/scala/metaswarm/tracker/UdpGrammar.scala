package metaswarm.tracker

import akka.util.ByteString
import fastparse.byte.all._
import metaswarm.misc.Util._

import scala.util.Try

/*
 * The grammar to parse all the server reply diagrams
 */
object UdpGrammar {
  import UdpDiagram._

  val connect = P(BS(ActionId.Connect.toBytes) ~/ BE.Int32 ~/ BE.Int64).map {
    case (transId, connId) =>
      ConnectReply(ActionId.Connect, transId, connId)
  }

  val announce = P(BS(ActionId.Announce.toBytes)
    ~/ BE.Int32
    ~/ BE.Int32
    ~/ BE.Int32
    ~/ BE.Int32
    ~/ (BE.Int32 ~/ BE.UInt16).rep).map {
    case (transId, interval, leechers, seeders,peers) =>
      AnnounceReply(ActionId.Announce, transId, interval, leechers, seeders, peers.toList)
  }

//  val scrape = P(BS(ActionId.Scrape.toBytes)
//    ~/ BE.Int32
//    ~/ (BE.Int32 ~/ BE.Int32 ~/ BE.Int32).rep).map {
//    case (transId, xs) =>
//      ScrapeReply(ActionId.Scrape, transId, xs.toVector)
//  }

  val error = P(BS(ActionId.Error.toBytes) ~/ BE.Int32 ~/ AnyByte.rep.!).map {
    case (transId, str) =>
      ErrorReply(ActionId.Error, transId, str.decodeUtf8.fold(throw _, identity))
  }

  val udpPacket = P(connect | announce /* | scrape */ | error)

  def parse(msg: ByteString): Option[UdpDiagram] = {
    Try(udpPacket.parse(msg.toBytes).get.value).toOption
  }
}
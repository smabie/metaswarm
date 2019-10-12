package metaswarm.tracker

import java.nio.ByteOrder

import akka.util.ByteString
import enumeratum._
import fastparse.byte.all._
import scalaz.syntax.std.option._

object UdpDiagram {
  implicit val networkOrder = ByteOrder.BIG_ENDIAN

  /*
   * The state of the download, sent to the tracker
   */
  sealed class EventId(val id: Int) extends EnumEntry

  object EventId extends Enum[EventId] {
    def apply(id: Int): EventId = {
      values.find(_.id == id) err "Can't find EventId"
    }

    val values = findValues

    case object None extends EventId(0)
    case object Completed extends EventId(1)
    case object Started extends EventId(2)
    case object Stopped extends EventId(3)

  }

  /* https://www.libtorrent.org/udp_tracker_protocol.html action = 0-3 */
  sealed class ActionId(val id: Int) extends EnumEntry {
    def toByteString: ByteString = {
      ByteString.newBuilder.putInt(id).result()
    }

    def toBytes: Bytes = Bytes.view(toArray)

    def toArray: Array[Byte] = toByteString.toArray
  }

  object ActionId extends Enum[ActionId] {
    def apply(id: Int): ActionId = {
      values.find(_.id == id) err "Can't find EventId"
    }

    val values = findValues

    case object Connect extends ActionId(0)

    case object Announce extends ActionId(1)

    case object Scrape extends ActionId(2)

    case object Error extends ActionId(3)

  }

  /*
 * The UDP tracker message that is sent to and from the tracker
 *
 * http://www.bittorrent.org/beps/bep_0015.html
 * https://www.libtorrent.org/udp_tracker_protocol.html
 *
 */
  sealed trait UdpDiagram {
    def toByteString: ByteString = ByteString.empty

    def transactionId: Int
  }

  case class ConnectSend(connectionId: Long = 0x41727101980L,
                         action: ActionId = ActionId.Connect,
                         transactionId: Int) extends UdpDiagram {
    override def toByteString = {
      ByteString.newBuilder
      .putLong(connectionId)
      .putInt(action.id)
      .putInt(transactionId)
      .result
      .compact
    }
  }

  case class ConnectReply(action: ActionId,
                          transactionId: Int,
                          connectionId: Long) extends UdpDiagram

  case class AnnounceSend(connectionId: Long,
                          action: ActionId,
                          transactionId: Int,
                          infoHash: ByteString, // 20 bytes
                          peerId: ByteString, // 20 bytes
                          downloaded: Long,
                          left: Long,
                          uploaded: Long,
                          event: EventId,
                          ip: Int,
                          key: Int,
                          numWant: Int,
                          port: Short,
                          extensions: Short) extends UdpDiagram {
    override def toByteString: ByteString = {
      ByteString.newBuilder
      .putLong(connectionId)
      .putInt(action.id)
      .putInt(transactionId)
      .putBytes(infoHash.toArray)
      .putBytes(peerId.toArray)
      .putLong(downloaded)
      .putLong(left)
      .putLong(uploaded)
      .putInt(event.id)
      .putInt(ip)
      .putInt(key)
      .putInt(numWant)
      .putShort(port)
      .putShort(extensions)
      .result()
      .compact
    }
  }

  case class AnnounceReply(action: ActionId,
                           transactionId: Int,
                           interval: Int,
                           leechers: Int,
                           seeders: Int,
                           peers: List[(Int, Int)]) extends UdpDiagram /* (ip, port) */

  case class ErrorReply(action: ActionId,
                        transactionId: Int,
                        errorString: String) extends UdpDiagram
}

//  case class ScrapeSend(connectionId: Long,
//                        action: ActionId,
//                        transactionId: Int) extends UdpDiagram {
//    override def toByteString: ByteString = {
//      ByteString.newBuilder
//        .putLong(connectionId)
//        .putInt(action.id)
//        .putInt(transactionId)
//        .result()
//    }
//  }
//
//  case class ScrapeInfo(infoHash: ByteString) extends UdpDiagram { // 20 bytes
//    override def toByteString: ByteString = {
//      ByteString.newBuilder.putBytes(infoHash.toArray).result
//    }
//  }
//
//  /* info_hash list (completed, downloaded, incomplete) */
//  case class ScrapeReply(action: ActionId,
//                         transactionId: Int,
//                         infoHashList: Vector[(Int, Int, Int)]) extends UdpDiagram
//

//}
//

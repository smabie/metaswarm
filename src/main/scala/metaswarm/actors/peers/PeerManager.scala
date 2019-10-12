package metaswarm.actors.peers

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, Timers}
import metaswarm.Config
import metaswarm.actors.file.FileIO
import metaswarm.actors.tracker.Tracker
import metaswarm.metainfo.MetaInfo
import metaswarm.misc.{MutBitVector, Util}
import metaswarm.peers.Peer.Peer
import metaswarm.peers.{Peer, PeerList}
import metaswarm.piece.PiecePicker
import metaswarm.pwp.WireDiagram
import metaswarm.pwp.WireDiagram.WireDiagram
import org.joda.time.DateTime

import scala.concurrent.duration._

object PeerManager {
  def start_!(meta: MetaInfo, field: MutBitVector, listener: ActorRef, fileIO: => ActorRef, tracker: => ActorRef)
             (implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new PeerManager(meta, field, listener, fileIO, tracker)), name = "peer-manager")
  }

  case class TransferData(var downloaded: Long = 0, var uploaded: Long = 0)

  sealed trait Messages

  case object GetPieceField extends Messages
  case object UpdateRequests extends Messages
  case object UpdateTrackerStats extends Messages
  case object RemoveStaleRequests extends Messages
  /* New peers found by the Tracker (Ip -> Port) */
  case class AddPeers(peers: List[(Int, Int)]) extends Messages
  case class PeerBitfield(peer: Peer, data: MutBitVector) extends Messages
  case class PeerMessage(diagram: WireDiagram) extends Messages
  case class HashMismatch(index: Int) extends Messages
  case class HashMatch(index: Int) extends Messages

}


class PeerManager(meta: MetaInfo,
                  field: MutBitVector,
                  listener: ActorRef,
                  fileIO: => ActorRef,
                  tracker: => ActorRef) extends Actor with ActorLogging with Timers {
  import PeerManager._

  List(("update-requests", UpdateRequests, 500 milliseconds),
    ("update-tracker-stats", UpdateTrackerStats, 1 minute),
    ("remove-stale-requests", RemoveStaleRequests, 1 seconds)
  ).foreach { case (key, msg, dur) => timers.startPeriodicTimer(key, msg, dur) }

  /* We send this data to the Tracker periodically */
  val transferData = TransferData()
  val peers = PeerList(PiecePicker(field, meta.pieces, meta.pieceSize, meta.totalSize))

  /*val peer = Peer("0.0.0.0", 51413)
  peers.addPeers_!(peer :: Nil)
  PeerClient.start_!(meta, peer, listener) ! PeerClient.Outbound*/

  def receive: Receive = {
    case RemoveStaleRequests =>
      peers.picker.sentRequests.removeStale_!(DateTime.now()).foreach { case (req, pc) =>
        val WireDiagram.Request(index, offset, size) = req

        log.info(s"Cancelling block with index: $index offset: $offset size: $size")
        pc ! PeerClient.OutboundData(WireDiagram.Cancel(index, offset, size))
      }

    case HashMatch(index) =>
      log.info(s"Piece $index hash matches with meta_info file. Sending Have messages...")
      peers.connectedPeers.keysIterator.foreach { pc =>
        pc ! PeerClient.OutboundData(WireDiagram.Have(index))
      }

    case HashMismatch(index) =>
      log.info(s"Piece $index cleared due to hash mismatch!")
      peers.picker.clearPiece_!(index)

    case UpdateTrackerStats =>
      val remaining = Util.bytesRemaining(meta, field)
      log.info(
        s"""
           | Update Tracker Stats Message
           | uploaded:   ${Util.formatMb(transferData.uploaded)}
           | downloaded: ${Util.formatMb(transferData.downloaded)}
           | remaining:  ${Util.formatMb(remaining)}
          """.stripMargin)

      tracker ! Tracker.TorrentStats(transferData.uploaded, transferData.downloaded, remaining)

    case UpdateRequests =>
      peers.fillRequests.foreach { case (req, actor) =>
        actor ! PeerClient.OutboundData(req)
      }

    case AddPeers(xs) =>
      val peerList = xs.map { case (ip, port) => Peer(ip, port) }

      log.info(
        s"""
           | Received Peers
           | ${peerList.mkString("\n")}
         """.stripMargin)


      //val peer = Peer("0.0.0.0", 6969)

      //val ys = peerList.take(10)
      //peers.addPeers_!(peerList.take(10))
      //peers.addPeers_!(ys)

      /*ys.foreach { peer =>
        PeerClient.start_!(meta, peer, listener) ! PeerClient.Outbound
      }*/
      val peer = Peer("0.0.0.0", 51413)
      peers.addPeers_!(peer :: Nil)
      PeerClient.start_!(meta, peer, listener) ! PeerClient.Outbound

    case GetPieceField =>
      val pc = sender()
      log.info(s"Sending PieceField to $pc")
      pc ! PeerClient.PieceField(field.clone())

    case PeerBitfield(peer, bvec) =>
      log.info(s"$peer Connected. Possesses ${bvec.highCount}/${bvec.length}")
      peers.peerConnected_!(sender(), peer, bvec)

    case PeerMessage(diagram) =>
      handleDiagram(diagram, sender())

    case x => log.error(s"Unhandled message $x")
  }

  def handleDiagram(diagram: WireDiagram, pc: ActorRef) = {
    val state = peers.state(pc)
    state.amChoking = false

    diagram match {
      case WireDiagram.UnChoke =>
        if (state.peerChoking) {
          log.info("Received UnChoke")
          peers.peerUnChoking_!(pc)
          peers.amInterested_!(pc)
        } else {
          log.info("Received UnChoke, but peer has already been unchoked! Ignoring...")
        }
        pc ! PeerClient.OutboundData(WireDiagram.UnChoke)
        pc ! PeerClient.OutboundData(WireDiagram.Interested)

      case WireDiagram.Choke =>
        if (!state.peerChoking) {
          log.info("Received Choke, removing peer from PiecePicker")
          peers.peerChoking_!(pc)
        } else {
          log.info("Received Choke, but peer has already been choked! Ignoring...")
        }

      case WireDiagram.Interested =>
        if (!state.peerInterested) {
          log.info("Received Interested")
          peers.peerInterested_!(pc)
        } else {
          log.info("Received Interested, but peer is already interested! Ignoring...")
        }

      case WireDiagram.NotInterested =>
        if (state.peerInterested) {
          log.info("Received NotInterested")
          peers.peerNotInterested_!(pc)
        } else {
          log.info("Received NotInterested, but peer is already not interested! Ignoring...")
        }

      case have @ WireDiagram.Have(index) =>
        log.info(s"Received Have for piece $index")
        peers.picker.addPeerPiece_!(pc, have)

      case piece @ WireDiagram.Piece(index, offset, bs) =>
        if (peers.picker.haveBlock(index, offset)) {
          log.info("Already have block, ignoring...")
        } else {
          //log.info("Received Piece, sending write to FileIO")

          /* Generate the Request that we sent so we can remove it */
          val req = WireDiagram.Request(index, offset, bs.length)
          peers.picker.sentRequests.removeRequest_!(req)


          /* Add the data downloaded to the transfer */
          transferData.downloaded += Config.RequestSize

          fileIO ! FileIO.Write(piece)

          /* If we now have a full piece, we need to verify the hash */
          if (peers.addBlock_!(pc, index, offset))
            fileIO ! FileIO.VerifyHash(index, meta.pieces(index))
        }

      case request: WireDiagram.Request if !state.amChoking =>
        log.info(s"Received Request, sending read to FileIO")

        transferData.uploaded += Config.RequestSize
        fileIO ! FileIO.Read(request)
    }
  }
}
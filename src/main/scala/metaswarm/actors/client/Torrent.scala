package metaswarm.actors.client

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Kill, Props}
import metaswarm.actors.file.FileIO
import metaswarm.actors.peers.PeerManager
import metaswarm.actors.tracker.Tracker
import metaswarm.metainfo.MetaInfo
import metaswarm.misc.{MutBitVector, Util}
import metaswarm.piece.PieceField

object Torrent {
  def start_!(torrentPath: String, filePath: String, trackerListener: ActorRef, peerListener: ActorRef)
             (implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new Torrent(torrentPath, filePath, trackerListener, peerListener)),
      name = s"torrent:${Util.actorName(torrentPath.replace('/', ':'))}:")
  }

  sealed trait Messages
  case object Start extends Messages
}

/*
 * Actor class responsible for managing all the connections and information
 * about a single metainfo (torrent) file. Each torrent actor manages a
 * PeerManager, a Tracker, and a FileIO actor.
 */
class Torrent(torrentPath: String,
              filePath: String,
              trackerListener: ActorRef,
              peerListener: ActorRef) extends Actor with ActorLogging {
  import Torrent._

  val meta = MetaInfo(torrentPath)

  val fileIO = FileIO.start_!(meta, filePath)

  val (peerManager: ActorRef, tracker: ActorRef) = {
    val field: MutBitVector = PieceField.load(meta, filePath) getOrElse {
      log.error(s"Error loading download file $filePath! Aborting...")
      self ! Kill
      MutBitVector.empty
    }
    PeerManager.start_!(meta, field, peerListener, fileIO, tracker) ->
      Tracker.start_!(meta, trackerListener, peerManager, Util.bytesRemaining(meta, field))
  }

  def receive: Receive = {
    case Start =>
      log.info("Sending Connect to Tracker")
      tracker ! Tracker.Connect

    case x => log.error(s"Unhandled message: $x")
  }
}
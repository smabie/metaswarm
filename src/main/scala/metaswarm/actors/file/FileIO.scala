package metaswarm.actors.file

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, Timers}
import akka.util.ByteString
import metaswarm.actors.client.file.PieceMappedBuffer
import metaswarm.actors.peers.PeerManager
import metaswarm.metainfo.MetaInfo
import metaswarm.misc.Util
import metaswarm.pwp.WireDiagram

import scala.concurrent.duration._

object FileIO {
  def start_!(meta: MetaInfo, path: String)(implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new FileIO(meta, path)), name = Util.actorName(s"file-io:${meta.name}:"))
  }

  sealed trait Messages

  /*
   * Requests a block of data.
   */
  case class Read(request: WireDiagram.Request) extends Messages

  /*
   * Writes a block of data.
   */
  case class Write(piece: WireDiagram.Piece) extends Messages

  /*
   * Flushes the data held in PieceMappedBuffer to disk.
   */
  case object Flush extends Messages

  /*
   * Asks the actor to verify the hash of the given piece.
   */
  case class VerifyHash(index: Int, hash: ByteString) extends Messages
}

/*
 * Manages the writing and reading of piece/block data to/from the filesystem.
 * Contains an off-heap mmap'ed buffer (PieceMappedBuffer) that handles the
 * actual filesystem access.
 */
class FileIO(meta: MetaInfo, path: String) extends Actor with ActorLogging with Timers {
  import FileIO._

  log.info("Starting...")

  val pmb = PieceMappedBuffer(meta, path)


  /* flush to disk every one minute */
  timers.startPeriodicTimer("flush-timer", Flush, 1 minute)

  def receive: Receive = {
    case VerifyHash(index, hash) =>
      sender() ! (if (pmb.verifyHash(index, hash)) {
        //log.info(s"Verified hash for piece $index")
        PeerManager.HashMatch(index)
      } else {
        log.info(s"Hash mismatch for piece $index")
        PeerManager.HashMismatch(index)
      })

    case Read(req @ WireDiagram.Request(index, offset, size)) =>
      pmb.read(index, offset, size).fold {
        log.error(s"Could not find data for $req!")
      }(sender() ! _)

    case Write(piece) =>
      if (!pmb.write_!(piece))
        log.error(s"Could not write block $piece!")
      //log.info(s"Wrote block with index ${piece.piece} and offset ${piece.offset}")

    case Flush =>
      log.info("Flushing file buffer!")
      pmb.flush()

    case x => log.error(s"Unhandled message $x")
  }
}

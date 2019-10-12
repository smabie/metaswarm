package metaswarm.piece

import akka.actor.ActorRef
import akka.util.ByteString
import metaswarm.Config
import metaswarm.misc.{LocalRand, MutBitVector}
import metaswarm.pwp.WireDiagram

import scala.collection.mutable.{Map => MMap, TreeSet => MTreeSet}


object PiecePicker {
  def apply(field: MutBitVector, hashes: Array[ByteString], pieceSize: Int, totalSize: Long) = {
    new PiecePicker(field, hashes, pieceSize, totalSize)
  }
}

/*
 * Primary data structure that holds the piece information for each torrent.
 * Contains our piece information, all of the peers' piece information, and
 * our partial pieces. Calculates which piece to request next based on
 * configurable strategies.
 */
class PiecePicker(field: MutBitVector, hashes: Array[ByteString], pieceSize: Int, totalSize: Long) extends LocalRand {
  private[this] def norm(offset: Int) = offset / Config.RequestSize

  private[this] def withQueueUpdate_!(piece: Piece): (Piece => Unit) => Unit = withQueueUpdate_!(piece, true)
  private[this] def withQueueUpdate_!(piece: Piece, update: Boolean)(fn: Piece => Unit): Unit = {
    if (update) {
      pieceQueue -= piece
      fn(piece)
      pieceQueue += piece
    } else fn(piece)
  }

  private[this] val peerFields = MMap.empty[ActorRef, MutBitVector]
  val pieceQueue = MTreeSet.empty[Piece](Piece.PartialPieceOrdering)

  val sentRequests = SentRequests()

  val pieceBlocks = pieceSize / Config.RequestSize
  val pieces = {
    val arr = new Array[Piece](field.length)
    var i = 0

    while (i < arr.length) {
      /* Only the last piece is of a different size */
      val size = if (i + 1 == arr.length) {
        (totalSize % pieceSize).toInt
      } else pieceSize

      arr(i) = Piece(i, size, Piece.State(field(i)), i == arr.length - 1)
      i += 1
    }
    arr
  }

  def addPeer_!(actor: ActorRef, vec: MutBitVector): Unit = {
    assert(field.length == vec.length, "Peer bitfield has different length than local bitfield!")
    assert(!peerFields.contains(actor), "Peer already added!")

    peerFields(actor) = vec
    vec.foreachWithIdx { (b, idx) =>
      if (b) withQueueUpdate_!(pieces(idx), !field(idx))(_.addPeer_!(actor))
    }
  }

  def removePeer_!(actor: ActorRef): Unit = {
    def err() = sys.error(s"No bit vector found for $actor!")

    peerFields.remove(actor).fold(err()) { bvec =>
      bvec.foreachWithIdx { (b, idx) =>
        if (b) withQueueUpdate_!(pieces(idx), !field(idx))(_.removePeer_!(actor))
      }
    }
  }

  def addPeerPiece_!(actor: ActorRef, have: WireDiagram.Have): Unit = {
    def err() = sys.error(s"Actor $actor not found for piece ${have.piece}")

    val WireDiagram.Have(index) = have
    peerFields.get(actor).fold(err()) { bvec =>
      if (!bvec(index)) {
        bvec.high_!(index)
        withQueueUpdate_!(pieces(index), !field(have.piece))(_.addPeer_!(actor))
      }
    }
  }

  def addBlock_!(index: Int, offset: Int): Boolean = {
    val piece = pieces(index)
    pieceQueue -= piece
    if (piece.addBlock_!(offset)) {
      field.high_!(index)
      true
    } else {
      pieceQueue += piece
      false
    }
  }

  /* If we have a hash mismatch we need to clear the piece */
  def clearPiece_!(index: Int) = {
    field.low_!(index)
    withQueueUpdate_!(pieces(index))(_.clear())
  }

  def haveBlock(index: Int, offset: Int): Boolean = {
    field(index) || (pieces(index).state match {
      case Piece.Complete      => true
      case Piece.Missing       => false
      case Piece.Partial(bvec) => bvec(norm(offset))
    })
  }

  def requestIter: Iterator[(WireDiagram.Request, Set[ActorRef])] = {
    pieceQueue.toIterator.flatMap(_.toRequests).filterNot { case (req, _) =>
      sentRequests.containsRequest(req)
    }
  }

  override def toString: String = {
    s"""
       | piece_size:     $pieceSize
       | blocks/piece:   $pieceBlocks
       |
       | bitfield:
       |  $field
       |  ${peerFields.values.mkString("\n  ")}
       |
       | pieces:
       |  ${pieces.mkString("\n  ")}
       |
       | queue:
       |  ${pieceQueue.mkString("\n  ")}
    """.stripMargin
  }
}


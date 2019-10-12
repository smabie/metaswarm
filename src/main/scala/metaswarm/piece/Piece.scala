package metaswarm.piece

import akka.actor.ActorRef
import metaswarm.Config
import metaswarm.misc.MutBitVector
import metaswarm.pwp.WireDiagram

import scala.collection.mutable.{Set => MSet}

object Piece {
  def apply(index: Int,  size: Int, state: State, last: Boolean = false) = new Piece(index, size, state, last)

  object InOrderPieceOrdering extends Ordering[Piece] {
    override def compare(x: Piece, y: Piece) = x.index compareTo y.index
  }

  /*
   * Prioritizes partial pieces over rare pieces. If there are no partial pieces
   * we select the rarest pieces. If then download in order if multiple pieces
   * have the same rarity.
   */
  object PartialPieceOrdering extends Ordering[Piece] {
    override def compare(x: Piece, y: Piece) =  {
      if (x == y) 0
      else if (x.state.isPartial && !y.state.isPartial) -1
      else if (!x.state.isPartial && y.state.isPartial) 1
      else x.index compareTo y.index
    }
  }

  object State {
    def apply(bool: Boolean): Piece.State = {
      if (bool)
        Piece.Complete
      else
        Piece.Missing
    }
  }

  sealed trait State extends Cloneable {
    def isPartial: Boolean = false
    def isComplete: Boolean = false
    def isMissing: Boolean = false

    override def clone(): State = this
  }

  case object Missing extends State {
    override def isMissing = true
  }

  case object Complete extends State {
    override def isComplete = true
  }

  case class Partial(field: MutBitVector) extends State {
    override def isPartial = true
    override def clone(): State =  this.copy(field.clone())
  }
}


class Piece(val index: Int, val size: Int, var state: Piece.State, last: Boolean) extends Cloneable {
  /* number of peers that have this piece */
  var count = 0

  /* list of peers that have this piece */
  val peers = MSet.empty[ActorRef]
  val blocks = math.ceil(size.toDouble / Config.RequestSize).toInt

  /*
   * Only the last block of the last piece will have a different sized request
   * size.
   */
  private[this] def reqSize(idx: Int) = {
    if (last && idx + 1 == blocks)
      size % Config.RequestSize
    else
      Config.RequestSize
  }

  def addPeer_!(actor: ActorRef) = {
    if (!peers.contains(actor)) {
      peers += actor
      count += 1
    }
  }

  def removePeer_!(actor: ActorRef) = {
    if (peers.contains(actor)) {
      peers -= actor
      count -= 1
    }
  }

  def addBlock_!(offset: Int): Boolean = {
    val blockOffset = offset / Config.RequestSize

    state match {
      case Piece.Missing =>
        val bvec = MutBitVector(blocks)
        state = Piece.Partial(bvec)
        bvec.high_!(blockOffset)

        /* Edge case where block = 1 */
        if (bvec.allHigh_?)
          state = Piece.Complete

      case Piece.Partial(bvec) =>
        bvec.high_!(blockOffset)

        if (bvec.allHigh_?)
          state = Piece.Complete

      case Piece.Complete =>
        sys.error("Piece already completed!")
    }

    state == Piece.Complete
  }

  /*
   * Return a list of requests that are matched to the set of actors that have
   * the data to fulfill the request. The PeerList then decides how to allocate
   * the requests based on a configurable strategy.
   */
  def toRequests: List[(WireDiagram.Request, Set[ActorRef])] = {
    val set = peers.toSet
    lazy val Nul = List.empty[(WireDiagram.Request, Set[ActorRef])]
    state match {
      case Piece.Missing =>
        (0 until blocks).map { blockIndex =>
          WireDiagram.Request(index, blockIndex * Config.RequestSize, reqSize(blockIndex)) -> set
        }(collection.breakOut)

      case Piece.Partial(bvec) =>
        var ret = Nul
        bvec.foreachWithIdx { (b, blockIndex) =>
          if (!b)
            ret = (WireDiagram.Request(index, blockIndex * Config.RequestSize, reqSize(blockIndex)) -> set) :: ret
        }
        ret

      case Piece.Complete => Nul
    }
  }

  def clear() = state = Piece.Missing

  override def toString: String = s"Piece: index: $index peers: $count state: $state"
  override def hashCode = index
}

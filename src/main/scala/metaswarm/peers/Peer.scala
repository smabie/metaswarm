package metaswarm.peers

import java.net.InetSocketAddress

import metaswarm.Config
import metaswarm.misc.{SocketAddress, MutBitVector, Util}
import scalaz.syntax.std.option._

object PeerState {
  def apply(field: MutBitVector): PeerState = new PeerState(field)
}

class PeerState(val field: MutBitVector) {
  var amChoking: Boolean = true
  var amInterested: Boolean = false
  var peerChoking: Boolean = true
  var peerInterested: Boolean = false

  /* Outstanding requests */
  var numReqs = 0

  /* The current size of request buffer */
  private[this] var _bufferSize = Config.InitialPeerBufferSize
  def bufferSize = _bufferSize
  def setBufferSize_!(n: Int) = _bufferSize = n
  def incrBufferSize_!(n: Int) = _bufferSize += n
  def decrBufferSize_!(n: Int) = {
    if (bufferSize - n < Config.InitialPeerBufferSize)
      _bufferSize = Config.InitialPeerBufferSize
    else
    _bufferSize -= n
  }

  def full_? = numReqs == _bufferSize
  def empty_? = numReqs == 0
}

object Peer {
  type Peer = InetSocketAddress

  def apply(ip: Int, port: Int): Peer = apply(Util.ipToString(ip), port)
  def apply(ip: String, port: Int): Peer  = SocketAddress(ip, port)
}

/*
 * A PeerScore determines which peers we like more. Note that field refers to
 * our Piece state, while the field in PeerState refers to their's.
 */
sealed abstract class PeerScore(field: MutBitVector, state: PeerState) {
  def apply(): Int
}

object PeerScore {
  /* I guess enumeratum doesn't work with classes? */
  private[this] val  values = Map(
    "peer-score-download" -> { (f: MutBitVector, s: PeerState) => new Download(f, s) }
  )

  def apply(name: String) = values.get(name) err "Can't find PeerScore"

  class Download(field: MutBitVector, state: PeerState) extends PeerScore(field: MutBitVector, state: PeerState) {
    def apply(): Int = state.bufferSize
  }
}



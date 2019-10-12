package metaswarm.peers

import akka.actor.ActorRef
import metaswarm.Config
import metaswarm.misc.Util.RichGrowShrink
import metaswarm.misc.{MutBitVector, Util}
import metaswarm.peers.Peer.Peer
import metaswarm.peers.PeerList.PeerInfo
import metaswarm.peers.PeerStrategy.LinearFill
import metaswarm.piece.PiecePicker
import metaswarm.pwp.WireDiagram

import scala.collection.mutable.{Map => MMap, Set => MSet}


object PeerList {
  /*
   * peer: The InetSocketAddress of the Peer
   * state: The symmetric choking and interested information
   * field:
   */
  case class PeerInfo(peer: Peer, state: PeerState, score: PeerScore)

  object PeerInfo {
    def apply(peer: Peer, field: MutBitVector, scorer: (MutBitVector, PeerState) => PeerScore) = {
      val state = PeerState(field)
      new PeerInfo(peer, state, scorer(field, state))
    }
  }

  def apply(picker: PiecePicker) = new PeerList(picker)
}

/*
 *
 */
class PeerList(val picker: PiecePicker) { self =>
  val strategy = PeerStrategy("linear-fill")(self) //PeerStrategy("linear-fill")(self)
  val scorer: (MutBitVector, PeerState) => PeerScore = PeerScore("peer-score-download")

  val knownPeers = MSet.empty[Peer]
  val connectedPeers = MMap.empty[ActorRef, PeerInfo]

  /* Total outstanding requests */
  var totalRequests = 0

  val (peerChokingSet, peerInterestedSet, amChokingSet, amInterestedSet) = {
    def Nul = MSet.empty[ActorRef]
    (Nul, Nul, Nul, Nul)
  }

  def rank() = {
    connectedPeers.filter { case (peer, _) =>
      amInterestedSet.contains(peer)
    }.values.toArray.sortBy(_.score())
  }

  def withConnected[A](actor: ActorRef)(fn: PeerInfo => A): A = {
    connectedPeers.get(actor).fold {
      sys.error(s"Can't find Peer $actor!")
    }(fn)
  }

  private[this] def peerChoke_!(bool: Boolean, actor: ActorRef): Unit = {
    state(actor).peerChoking = bool
    peerChokingSet.change(bool, actor)
  }

  private[this] def peerInterest_!(bool: Boolean, actor: ActorRef): Unit = {
    state(actor).peerInterested = bool
    peerInterestedSet.change(bool, actor)
  }

  private[this] def amChoke_!(bool: Boolean, actor: ActorRef): Unit = {
    state(actor).amChoking = bool
    amChokingSet.change(bool, actor)
  }

  private[this] def amInterest_!(bool: Boolean, actor: ActorRef): Unit = {
    val x = amInterestedSet
    state(actor).amInterested = bool
    amInterestedSet.change(bool, actor)
  }

  def addPeers_!(peers: List[Peer]): Unit = knownPeers ++= peers

  def peerConnected_!(actor: ActorRef, peer: Peer, field: MutBitVector): Unit = {
    val pi = PeerInfo(peer, field, scorer)
    connectedPeers += actor -> pi

    if (pi.state.amChoking)
      amChokingSet += actor
    if (pi.state.amInterested)
      amInterestedSet += actor
    if (pi.state.peerChoking)
      peerChokingSet += actor
    if (pi.state.peerInterested)
      peerInterestedSet += actor

    picker.addPeer_!(actor, field)
  }

  def peerDisconnected_!(actor: ActorRef): Unit = connectedPeers -= actor

  def peerChoking_!(actor: ActorRef) = peerChoke_!(true, actor)
  def peerUnChoking_!(actor: ActorRef) = peerChoke_!(false, actor)

  def peerInterested_!(actor: ActorRef) = peerInterest_!(true, actor)
  def peerNotInterested_!(actor: ActorRef) = peerInterest_!(false, actor)

  def amChoking_!(actor: ActorRef)= amChoke_!(true, actor)
  def amUnChoking_!(actor: ActorRef) = amChoke_!(false, actor)

  def amInterested_!(actor: ActorRef)= amInterest_!(true, actor)
  def amNotInterested_!(actor: ActorRef) = amInterest_!(false, actor)

  /* The peers that I can download from */
  def targetPeers = amInterestedSet.toSet &~ peerChokingSet

  def state(actor: ActorRef) = withConnected(actor)(_.state)

  /* Average number of requests */
  def averageRequests(target: Set[ActorRef]): Double = totalRequests.toDouble / target.size
  def averageRequests: Double = averageRequests(targetPeers)

  def incrRequests_!(actor: ActorRef): Unit = incrRequests_!(state(actor))
  def incrRequests_!(state: PeerState): Unit = {
    totalRequests += 1
    state.numReqs += 1
  }

  def decrRequests_!(actor: ActorRef): Unit = decrRequests_!(state(actor))
  def decrRequests_!(state: PeerState): Unit = {
    totalRequests -= 1
    state.numReqs -= 1
  }

  def addBlock_!(actor: ActorRef, index: Int, offset: Int) = {
    decrRequests_!(actor)
    picker.addBlock_!(index, offset)
  }

  def fillRequests: Iterator[(WireDiagram.Request, ActorRef)] = strategy.apply
}
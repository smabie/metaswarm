package metaswarm.peers

import akka.actor.ActorRef
import enumeratum._
import metaswarm.Config
import metaswarm.misc.LocalRand
import metaswarm.misc.Util.ToMutableSet
import metaswarm.pwp.WireDiagram
import org.joda.time.DateTime
import scalaz.syntax.std.option._
import scala.collection.mutable.{Set => MSet}

/*
 * The PeerStrategy classes are at the heart of our configurable piece and peer
 * request strategy. These objects are given the currently known state of all
 * the peers and all the pieces. It is up to this class, given all of the
 * available information, how we should allocate the requests, and how many to
 * be allocated. Because of this, these objects have a lot of complexity and
 * nuance. Each strategy needs to do different and varied things depending on
 * the goal of the particular strategy.
 */
sealed abstract class PeerStrategy(peers: PeerList) {
  def addRequest_!(req: WireDiagram.Request, actor: ActorRef, dt: DateTime) = {
    peers.picker.sentRequests.addRequest_!(req, dt, actor)
    peers.incrRequests_!(actor)
  }

  def apply: Iterator[(WireDiagram.Request, ActorRef)]
}

object PeerStrategy {
  val values: Map[String, PeerList => PeerStrategy] = Map(
    "linear-fill" -> { p: PeerList => new LinearFill(p) },
    "random-average" -> { p: PeerList => new RandomAverage(p) }
  )

  def apply(name: String) = values.get(name) err "Can't find PeerStrategy"

  /*
   * We linearly allocate requests to peers based on how quickly they are
   * fulfilling them. We increase the buffer size by PeerBufferStep if they have
   * zero outstanding requests and decrease if half of their current bufferSize
   * is unfilled.
   */
  class LinearFill(peers: PeerList) extends PeerStrategy(peers) {
    private[this] def adjustBufferSize_!(active: MSet[ActorRef]) = {
      val step = Config.PeerBufferStep

      active.foreach { actor =>
        val state = peers.state(actor)
        if (state.numReqs == 0)
          state.incrBufferSize_!(step)
        else if (state.numReqs > state.bufferSize * Config.LinearFillReduceRatio)
          state.decrBufferSize_!(step)
      }
    }

    def apply: Iterator[(WireDiagram.Request, ActorRef)] = {
      val active = peers.targetPeers.filterNot(peers.state(_).full_?).toMutable

      var (miss, hit) = 0 -> 0

      adjustBufferSize_!(active)
      peers.picker.requestIter.flatMap { case (req, actorRefs) =>

        /* If we have a request that an active peer can't fill, we penalize by lowering their buffer size */
        (active &~ actorRefs).foreach {
          peers.state(_).decrBufferSize_!(Config.LinearFillMissPenalty)
        }
        (actorRefs & active).find(!peers.state(_).full_?).map(req -> _).fold {
          miss += 1
          Option.empty[(WireDiagram.Request, ActorRef)]
        } { x  =>
          hit += 1
          Some(x)
        }
      }.takeWhile { case (req, actor) =>
        if (miss.toDouble / (miss + hit) > Config.LinearFillCost)
          false
        else {
          /* Remove full peer from allocation list */
          if (peers.state(actor).full_?)
            active -= actor
          else
            addRequest_!(req, actor, DateTime.now())

          /* If all the peers are full we can just stop now */
          active.nonEmpty
        }
      }
    }
  }

  /*
   * We randomly allocate requests to all of our target peers. The max number
   * of outstanding requests at any one time is controlled by bufferSize. We
   * dynamically allocate the bufferSize based on how quickly the target peers
   * are consuming them. This is sub-optimal when we have a mix of fast and slow
   * peers, since the slow peers will receive too many requests too quickly and
   * the fast peers won't receive enough.
   */
  class RandomAverage(peers: PeerList) extends PeerStrategy(peers) with LocalRand {
    private[this] val adjustBufferSize_! = {
      var bufferSize = Config.InitialPeerBufferSize
      val step = Config.PeerBufferStep
      (avg: Double, set: Set[ActorRef]) => {
        if (avg < 1)
          bufferSize += step
        else if (avg > bufferSize / 2)
          bufferSize -= step

        /* This isn't really necessary since each peer has the same buffer size */
        set.foreach(peers.state(_).setBufferSize_!(bufferSize))
        bufferSize
      }
    }

    def apply: Iterator[(WireDiagram.Request, ActorRef)] = {
      val set = peers.targetPeers
      val numPeers = set.size
      var avg = peers.averageRequests(set)

      val newSize = adjustBufferSize_!(avg, set)

      peers.picker.requestIter.flatMap { case (req, actorRefs) =>
        val arr = (actorRefs & set).toArray
        if (arr.isEmpty) {
          None
        } else {
          val actor = arr(Rand.nextInt(arr.length))
          Some(req -> actor)
        }
      }.takeWhile { case (req, actor) =>
        if (avg >= newSize)
          false
        else {
          /*
           * These side-effects are safe since we always return true if this
           * branch is taken. We can't do it in the above flatMap because then
           * we will always add and increment one extra request each time. This
           * isn't very noticeable when the request buffer is large, but it
           * becomes very bad if it's small.
           */
          addRequest_!(req, actor, DateTime.now())
          avg += 1d / numPeers
          true
        }
      }
    }
  }
}


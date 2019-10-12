package metaswarm.actors.client

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}
import metaswarm.actors.peers.PeerListener
import metaswarm.actors.tracker.TrackerListener

object ListenerSupervisor {
  def start_!(implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new ListenerSupervisor()), name = "listener-supervisor")
  }

  sealed trait Messages

  /*
   * Start the tracker and peer listeners.
   */
  case object Start extends Messages

  /*
   * Returns the ActorRef handles for both the tracker and the peer listeners.
   */
  case object GetListenerRefs extends Messages

  /*
   * Stop the underlying tracker and peer listeners.
   */
  case object Stop extends Messages
}

/*
 * This actor is responsible for managing the TrackerListener and the
 * PeerListener. The udpPort is the port for the TrackerListener and the
 * tcpPort is the port for the PeerListener
 */
class ListenerSupervisor extends Actor with ActorLogging {
  import ListenerSupervisor._

  def receive: Receive = {
    case Start =>
      log.info("Starting Listeners...")
      val tracker = TrackerListener.start_!
      val peer = PeerListener.start_!

      context become ready(tracker, peer)

    case x =>
      log.info(s"Unhandled message $x")
  }

  def ready(trackerListener: ActorRef, peerListener: ActorRef): Receive  = {
    case GetListenerRefs =>
      sender() ! (trackerListener, peerListener)

    case x =>
      log.info(s"Unhandled message $x")
  }
}
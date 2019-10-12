package metaswarm.actors.tracker

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Kill, Props}
import akka.io.{IO, Udp}
import metaswarm.Config
import metaswarm.misc.SocketAddress
import metaswarm.misc.Util._
import metaswarm.tracker.UdpDiagram.UdpDiagram
import metaswarm.tracker.UdpGrammar

object TrackerListener  {
  def start_!(implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new TrackerListener()), name = "tracker-listener")
  }

  sealed trait Messages

  case class Send(data: UdpDiagram, remote: InetSocketAddress) extends Messages
}

/*
 * There is only one TrackerListener, each tracker actor maps 1:1 for each
 * torrent and routes their messages to UdpListener. This way, we only need to
 * use one UDP port instead of a bunch.
 */
class TrackerListener extends Actor with ActorLogging {
  import context.system

  val port = Config.TrackerUdpPort

  IO(Udp) ! Udp.Bind(self, SocketAddress(Config.LocalAddress, port))

  def receive: Receive = {
    case Udp.Bound(_) =>
      log.info(s"TrackerListener on UDP port $port")
      context become ready(sender(), Map.empty[Int, ActorRef])

    case x =>
      log.info(s"$x Error in binding UDP port $port")
      self ! Kill
  }

  def ready(conn: ActorRef, senderMap: Map[Int, ActorRef]): Receive  = {
    def updateSender_!(newSenderMap: Map[Int, ActorRef]) = {
      context become ready(conn, newSenderMap)
    }

    PartialFunction {
      /* Send the diagram from the Tracker to the conn */
      case TrackerListener.Send(diagram, remote) =>
        val bytes = diagram.toByteString

        log.info(
          s"""
             | Supervisor Message
             | host:    ${remote.getHostName}
             | diagram: $diagram
         """.stripMargin)
        conn ! Udp.Send(bytes, remote)

        updateSender_!(senderMap + (diagram.transactionId -> sender()))

      /* Send the parsed bytes from the Listener to the Tracker */
      case Udp.Received(bs, remote) =>
        UdpGrammar.parse(bs).fold {
          log.error("Malformed message received, ignoring...")
        } { diagram =>
          val transId = diagram.transactionId

          senderMap.get(transId).fold {
            log.error(s"Could not forward message to Tracker, no transactionId $transId found")
          } { tracker =>
            log.info(
              s"""
                 | UDP Listener Message
                 | host:    ${remote.getHostName}
                 | bytes:   ${bs.toHex}
                 | diagram: $diagram
                """.stripMargin)
            tracker ! Tracker.TrackerMessage(diagram, remote)

            updateSender_!(senderMap - transId)
          }
        }

      case x @ Udp.Unbind =>
        log.info("Unbind message received")
        conn ! x

      case Udp.Unbound =>
        log.info("Received Unbound message. Killing actor...")
        self ! Kill

      case x => log.error(s"Unhandled message: $x")
    }
  }
}

package metaswarm.actors.tracker

import java.net.{InetSocketAddress, URI}

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Kill, Props, Timers}
import akka.util.ByteString
import metaswarm.Config
import metaswarm.actors.peers.PeerManager
import metaswarm.metainfo.MetaInfo
import metaswarm.misc.{LocalRand, Util}
import metaswarm.tracker.UdpDiagram
import metaswarm.tracker.UdpDiagram.{ActionId, EventId, UdpDiagram}

import scala.concurrent.duration._

object Tracker {
  def start_!(meta: MetaInfo, listener: ActorRef, peerManager: => ActorRef, bytesLeft: Long)
             (implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new Tracker(meta, listener, peerManager, bytesLeft)),
      name = Util.actorName(s"tracker:${meta.file.name}"))
  }

  type ConnectionMap = Map[String, Tracker.ConnectionInfo]
  case class ConnectionInfo(connectionId: Long, port: Int)

  sealed trait Messages

  /*
   * Start up all child actors and try to connect to their corresponding trackers
   */
  case object Connect extends Messages

  /*
   * One of the child's UDP connection is ready to be used
   */
  case class Connected(url: String) extends Messages

  /*
   * Send from the child listener after parsing
   */
  case class TrackerMessage(data: UdpDiagram, remote: InetSocketAddress) extends Messages

  case class TorrentStats(uploaded: Long, downloaded: Long, left: Long) extends Messages
  /*
   * Send an AnnounceSend message to the remote host
   */
  case class Announce(remote: InetSocketAddress) extends Messages
}

class Tracker(meta: MetaInfo,
              listener: ActorRef,
              peerClientPool: => ActorRef,
              bytesLeft: Long) extends Actor with Timers with ActorLogging with LocalRand {
  import Tracker._

  var (uploaded, downloaded, left) = (0L, 0L, bytesLeft)
  val event: EventId = EventId.None
  val ipAddress = 0
  val key = Rand.nextInt()
  val numWant = -1
  val extensions: Short = 0

  val infoHash = ByteString(meta.infoHash.toArray)

  val receive = ready(Map.empty[String, ConnectionInfo])

  def updateConnections_!(connections: ConnectionMap) = context.become(ready(connections))

  def ready(connections: ConnectionMap): Receive = {
    case TorrentStats(up, down, remaining) =>
      uploaded = up
      downloaded = down
      left = remaining

    /* Send the ConnectSend msg to the listener */
    case Connect =>
      meta.announceList.foreach { case x :: _ => /* Just take the first url in the tracker group */
        val uri = new URI(x)
        val (host, port) = (uri.getHost, if (uri.getPort == -1) 80 else uri.getPort)
        val remote = new InetSocketAddress(host, port)

        val data = UdpDiagram.ConnectSend(transactionId = Rand.nextInt())
        listener ! TrackerListener.Send(data, remote)
      }

      /* (re)-announce to tracker */
    case Announce(remote) =>
      val host = remote.getHostName

      connections.get(host).fold(sys.error(s"No connection for $host found!")) {
        case ConnectionInfo(connId, _) =>
          val diagram = genAnnounce(connId, ActionId.Announce, Rand.nextInt())
          listener ! TrackerListener.Send(diagram, remote)
      }

    /* The parsed diagram send from the child listener */
    case TrackerMessage(diagram, remote) =>
      log.info(
        s"""
           | Tracker Message
           | host:    ${remote.getHostName}
           | diagram: $diagram
         """.stripMargin)
      handleDiagram(diagram, remote, connections)

    case x =>
      log.error(s"Unhandled message: $x")
      self ! Kill
  }

  def genAnnounce(connId: Long, action: ActionId, transId: Int) = {
    UdpDiagram.AnnounceSend(
      connId,
      ActionId.Announce,
      transId,
      infoHash,
      Config.PeerId,
      downloaded,
      left,
      uploaded,
      event,
      ipAddress,
      key,
      numWant,
      Config.PeerTcpPort.toShort,
      extensions)
  }

  def handleDiagram(diagram: UdpDiagram, remote: InetSocketAddress, connections: ConnectionMap): Unit = diagram match {
    case UdpDiagram.ConnectReply(ActionId.Connect, _, connId) =>
      val (host, port) = remote.getHostName -> remote.getPort
      val connInfo = ConnectionInfo(connId, port)

      val diagram = genAnnounce(connId, ActionId.Announce, Rand.nextInt())
      listener ! TrackerListener.Send(diagram, remote)

      updateConnections_!(connections + (host -> connInfo))

    case UdpDiagram.AnnounceReply(ActionId.Announce, _, interval, leechers, seeders, peers) =>
      log.info(
        s"""
           | Tracker Announce Reply
           | interval: $interval
           | leechers: $leechers
           | seeders:  $seeders
          """.stripMargin)

      peerClientPool ! PeerManager.AddPeers(peers)
      timers.startSingleTimer(Rand.nextInt(), Announce(remote), interval seconds)

    case x => log.error(s"Unhandled diagram: $x")
  }
}


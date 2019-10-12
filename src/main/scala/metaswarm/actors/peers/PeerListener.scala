package metaswarm.actors.peers

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Kill, Props, Timers}
import akka.io.{IO, Tcp}
import akka.util.ByteString
import metaswarm.Config
import metaswarm.misc.SocketAddress
import metaswarm.misc.Util._
import metaswarm.pwp.WireGrammar
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._

object PeerListener {
  type PeerRequests = Map[InetSocketAddress, Set[ActorRef]]
  type PeerConnections = Map[ActorRef, ActorRef]
  type InfoHashConnections = Map[ByteString, ActorRef]
  type UnknownConnections = Set[ActorRef]

  /*
   * ConnectionInfo keeps track of the states of all the peer connections.
   *
   *  peerRequests: IpAddress -> Set[PeerClient]
   *                keeps track of outgoing peers that we have created a
   *                PeerClient for. We have sent a Connect message but they
   *                have not responded yet. Since it's possible to have multiple
   *                peers for different torrents that have the same address, we
   *                might need to keep track of multiple PeerClients for a
   *                single address.
   *
   * infoHashConns: info_hash -> Internal TCP Actor
   *                incoming peers that we have gotten the info_hash of. We have
   *                not yet confirmed that we possess an active torrent with the
   *                given info_hash.
   *
   * unknownConns:  Internal TCP Actor
   *                incoming peers that have connected to us but that we know
   *                nothing else about them.
   *
   * peerConns:     Internal TCP Actor -> PeerClient
   *                peers that have we have already sent a HandShake to. If it's
   *                an outgoing peer we might still to receive the HandShake.
   *
   * Outgoing peers lifecycle: peerRequests -> peerConns
   * Incoming peers lifecycle: unknownConns -> infoHashConns -> peerConns
   */
  case class ConnectionInfo(peerRequests: PeerRequests,
                            peerConns: PeerConnections,
                            infoHashConns: InfoHashConnections,
                            unknownConns: UnknownConnections)
  object ConnectionInfo {
    def apply(): ConnectionInfo = ConnectionInfo(Map.empty, Map.empty, Map.empty, Set.empty)
  }

  def start_!(implicit context: ActorContext): ActorRef = {
    context.actorOf(Props(new PeerListener()), name = "peer-listener")
  }

  sealed trait Messages

  /*
   * When the PeerClient wants to connect to a peer it sends this message to
   * the PeerListener. The PeerListener creates the connection and when
   * connected and then responds with the PeerConnectionCreated message. We
   * need both the info_hash and socket address because a socket address could
   * be hosting multiple torrents.
   */
  case class CreatePeerConnection(remote: InetSocketAddress) extends Messages

  /*
   * Sent from a PeerClient when a torrent with the info_hash is found and also
   * we are accepting connections
   */
  case class TorrentFound(infoHash: ByteString) extends Messages

  /*
   * Sent from a PeerClient when a torrent with the info_hash does not exist
   * or we are not accepting connections
   */
  case class UnknownTorrent(infoHash: ByteString) extends Messages
}

/*
 * The PeerListener accepts incoming peer connections and adds it to it's local
 * connection pool. Once the HandShake has been received, we either close the
 * connection or create a PeerClient under the appropriate tracker.
 */
class PeerListener extends Actor with Timers with ActorLogging {
  import PeerListener._
  import context.system

  val port = Config.PeerTcpPort

  def become_!(pr: PeerRequests = null,
               pc: PeerConnections = null,
               ic: InfoHashConnections = null,
               uc: UnknownConnections = null)
              (implicit conn: ConnectionInfo) = {
    context become
      ready(conn.copy(
        peerRequests = Option(pr) | conn.peerRequests,
        peerConns = Option(pc) | conn.peerConns,
        infoHashConns = Option(ic) | conn.infoHashConns,
        unknownConns = Option(uc) | conn.unknownConns))
  }

  IO(Tcp) ! Tcp.Bind(self, SocketAddress(Config.LocalAddress, port))

  def receive: Receive = {
      case Tcp.Bound(_) =>
        log.info(s"PeerListener bound on TCP port $port")
        context become ready(ConnectionInfo())

      case x =>
        log.info(s"$x Error in binding TCP port $port")
        self ! Kill
  }

  def ready(implicit info: ConnectionInfo): Receive = {
    /*
     *************************
     * DEFAULT CASE STATEMENTS
     *************************
     */
    case Tcp.Received(bs) if info.peerConns.contains(sender()) =>
      //log.info(s"Received data from remote TCP actor, forwarding to $peerClient")
      info.peerConns(sender()) forward PeerClient.InboundData(bs)

    /*
     ***********************************
     * INCOMING HANDLING CASE STATEMENTS
     ***********************************
     * Extra work is required for connections that originate from a remote host.
     * These first case statements deal with sitting up a new incoming
     * connection. Once the connection is set up it is handled just like normal
     * outgoing connections.
     */

    /* A new connection */
    case Tcp.Connected(remote, _) if !info.peerRequests.contains(remote) =>
      log.info("New incoming connection received")
      sender() ! Tcp.Register(self)
      become_!(uc = info.unknownConns + sender())

    /* we need to get the handshake and then find the torrent or drop the connection */
    case Tcp.Received(bs) if info.unknownConns(sender()) =>
      WireGrammar.parseHandshake(bs).fold {
        log.error("Closing connection for invalid handshake")
        sender() ! Tcp.Close
        become_!(uc = info.unknownConns - sender())
      } { handshake =>
        // find torrent with same infoHash. if it exists PeerClient is created and then
        // a TorrentFound message is sent to the PeerListener. If not, UnknownTorrent
        become_!(ic = info.infoHashConns + (handshake.infoHash -> sender()), uc = info.unknownConns - sender())
      }

    /* Sent from a PeerClient */
    case UnknownTorrent(infoHash) =>
      info.infoHashConns.get(infoHash).fold {
        log.error(s"No info_hash found for ${infoHash.toHex} from UnknownTorrent")
      } { conn =>
        log.info(s"Closing connection for ${infoHash.toHex} of $conn")
        conn ! Tcp.Close
        become_!(ic = info.infoHashConns - infoHash)
      }

    /* Sent from a PeerClient */
    case TorrentFound(infoHash) =>
      info.infoHashConns.get(infoHash).fold {
        log.error(s"TorrentFound but info_hash missing for ${infoHash.toHex}. Skipping...")
      } { conn =>
        log.info(s"TorrentFound for info_hash ${infoHash.toHex}. Notifying PeerClient")
        // send information to peerClient
        // sender() ! PeerConnectionCreated(conn)
        become_!(ic = info.infoHashConns - infoHash, pc = info.peerConns + (conn -> sender()))
      }


    /*
     ***********************************
     * OUTGOING HANDLING CASE STATEMENTS
     ***********************************
     */
    case CreatePeerConnection(remote) =>
      log.info(s"${sender()} requests connection on $remote")

      IO(Tcp) ! Tcp.Connect(remote)

      /* update the set or add a new set with one element */
      val set = info.peerRequests.get(remote).fold(Set(sender()))(_ + sender())
      become_!(pr = info.peerRequests.updated(remote, set))

    case Tcp.Connected(remote, _) if info.peerRequests.contains(remote) =>
      val set = info.peerRequests(remote)
      val peerClient = set.head

      log.info(s"Connected to $remote on behalf of $peerClient")

      sender() ! Tcp.Register(self)
      peerClient ! PeerClient.PeerConnectionCreated(sender())

      val pr = (set.size == 1) ? (info.peerRequests - remote) | info.peerRequests.updated(remote, set.tail)
      become_!(pr = pr, pc = info.peerConns + (sender() -> peerClient))

    case x =>
      log.error(s"Unhandled message $x")
  }
}
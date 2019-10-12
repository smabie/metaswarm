package metaswarm.actors.peers

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Kill, Props, Timers}
import akka.io.Tcp
import akka.util.ByteString
import metaswarm.metainfo.MetaInfo
import metaswarm.misc.MutBitVector
import metaswarm.peers.Peer.Peer
import metaswarm.pwp.WireDiagram.WireDiagram
import metaswarm.pwp.{WireDiagram, WireGrammar}
import metaswarm.misc.Util.RichByteString

object PeerClient {
  def start_!(meta: MetaInfo, peer: Peer, listener: ActorRef)
             (implicit context: ActorContext) = {
    context.actorOf(Props(new PeerClient(meta, peer, listener)),
      name = s"peer-client:${peer.toString.drop(1)}:")

  }

  sealed trait Messages

  /*
   * When we have successfully connected to the remote peer via the
   * PeerListener, we will get this message We then can send messages directly
   * to the connection actor, though we will receive messages (forwarded)
   * from PeerListener.
   */
  case class PeerConnectionCreated(conn: ActorRef) extends Messages

  /*
   * We are responding to a peer that has already sent us a handshake. This
   * differs from PeerConnectionCreated because we can skip to the bitfield
   * phase instead of waiting for their handshake.
   */
  case class PeerConnectionResponse(conn: ActorRef) extends Messages

  /*
   * The first message a PeerClient needs to receive is either the Outbound or
   * Inbound message. These messages denote whether the PeerClient has been
   * created by us (the client) in response to an outgoing connection request,
   * or whether a peer has proactively connected to us. The procedures are
   * different for each state since an inbound connection could ask for a
   * torrent that we don't possess.
   */
  case object Outbound extends Messages
  case object Inbound extends Messages

  /*
   * InboundData is forwarded by the PeerListener from the underlying TCP IO
   * actor managed by the PeerListener. The PeerClient first parses the message
   * (which may not be complete) and when a full message is available, sends the
   * appropriate message to the PeerManager. Usually this will be a PeerMessage.
   */
  case class InboundData(bs: ByteString) extends Messages

  /*
   * Data that is sent by the PeerManager intended to be serialized and
   * forwarded to the TCP IO actor managed by the PeerListener.
   */
  case class OutboundData(diagram: WireDiagram) extends Messages

  /*
   * Sent by the PeerManager after the PeerClient requests it with
   * GetPieceField.
   */
  case class PieceField(field: MutBitVector) extends Messages
}

/*
 * Each peerClient handles one peer. This peer sends data through the
 * PeerListener, and the PeerClient sends the data back directly.
 */
class PeerClient(meta: MetaInfo,
                 peer: Peer,
                 listener: ActorRef) extends Actor with Timers with ActorLogging {
  import PeerClient._
  log.info("PeerClient created")

  val handshake = Tcp.Write(WireDiagram.HandShake(meta.infoHash).toByteString)

  def receive: Receive = {
    case Outbound =>
      log.info("Outbound PeerClient created, asking for peer connection")
      listener ! PeerListener.CreatePeerConnection(peer)
      context become setup()

    /*
     * Since we were created, we know the torrent has been found. So we send the
     * TorrentFound message
     */
    case Inbound =>
      log.info("Inbound PeerClient created, sending torrent information")
      listener ! PeerListener.TorrentFound(meta.infoHash)
      context become setup()

    case x => log.error(s"Unhandled message $x")
  }

  def setup(): Receive = {
    case PeerConnectionCreated(conn) =>
      log.info(s"Sending HandShake with info_hash: ${meta.infoHash.toHex} to $conn")
      conn ! handshake
      context become handshake(conn)

    /* We are responding to the a handshake so we skip the handshake stage and go directly to the bitfield */
    case PeerConnectionResponse(conn) =>
      log.info("Sending HandShake in response to incoming peer HandShake")
      conn ! handshake
      context become bitField(conn)

    case x => log.error(s"Unhandled message $x")
  }

  def handshake(conn: ActorRef): Receive = {
    case InboundData(bs) =>
      WireGrammar.parseHandshake(bs).fold {
        log.error(s"len: ${bs.length} ${bs.toHex}")
        log.error("Malformed handshake received, dropping connection")
        conn ! Tcp.Close
        self ! Kill
      } { case WireDiagram.HandShake(peerHash, _) =>
          if (meta.infoHash != peerHash) {
            log.error(
              s"""
                | Dropping connection for info_hash mismatch
                | local info_hash:  ${meta.infoHash.toHex}
                | remote info_hash: ${peerHash.toHex}
              """.stripMargin
            )
            conn ! Tcp.Close
            self ! Kill
          } else {
            log.info(s"Handshake received with matching info_hash ${meta.infoHash.toHex}")

            context.parent ! PeerManager.GetPieceField
            context become bitField(conn)
          }
      }
    case x => log.error(s"Unhandled message $x")
  }

  def bitField(conn: ActorRef): Receive = {
    /* when both are true, we can transition out */
    var (received, sent) = false -> false

    /*
     * Since the bitfield is optional, we might get a different message that
     * we then need to handle after the state transition. We store this message
     * in optData and then send it to our self after the transition.
     */
    var optData = Option.empty[ByteString]
    var buf = ByteString.empty

    def transition_!() = {
      val data = optData getOrElse ByteString.empty
      context become ready(conn, data)
    }

    PartialFunction {
      case PieceField(field) =>
        log.info("Sending BitField message to peer")
        conn ! WireDiagram.BitField(field.toUnsafeArray)

        sent = true
        if (received) transition_!()

      case PeerClient.InboundData(bs) =>
        buf = buf ++ bs
        WireGrammar.parseFast(buf).fold({ error =>
          if (error) {
            log.error("Could not parse message. Killing PeerClient...")
            conn ! Tcp.Close
            self ! Kill
          } else {
            log.info("Received data, waiting for more...")
          }
        }, { case (diagram, index) =>
          val bvec = diagram match {
            case WireDiagram.BitField(data) =>
              log.info("Received BitField from peer.")
              if (index < buf.length)
                optData = Some(buf.drop(index))
              MutBitVector(data, meta.numPieces)

            case _ =>
              log.info("BitField skipped, assuming peer has no pieces")
              optData = Some(buf)
              MutBitVector(meta.numPieces)
          }
          context.parent ! PeerManager.PeerBitfield(peer, bvec)

          received = true
          if (sent) transition_!()
        })
    }
  }

  /*
   * This is the main handler after we have validated the handshake and
   * bit_field. If the bit_field wasn't set and instead we received a different
   * message, it is stored in the pre ByteString. If we did receive a bit_field
   * then this variable will be empty.
   */
  def ready(conn: ActorRef, pre: ByteString): Receive = {
    def recParse(buf: ByteString): ByteString = {
      if (buf.isEmpty) buf
      else WireGrammar.parseFast(buf).fold({ error =>
        if (error) {
          log.error("Could not parse message. Killing actor...")
          conn ! Tcp.Close
          self ! Kill
        } else log.info("Received data, waiting for more...")
        buf
      }, { case (diagram, index) =>
        diagram match {
          case WireDiagram.KeepAlive =>
          case _ =>
            //log.info(s"Parsed diagram $diagram, sending to PeerManager")
            context.parent ! PeerManager.PeerMessage(diagram)
        }
        recParse(buf.drop(index))
      })
    }

    var buf = pre
    PartialFunction {
      case InboundData(bs) =>
        buf = recParse(buf ++ bs)

      case OutboundData(diagram) =>
        //log.info(s"sending $diagram")
        conn ! Tcp.Write(diagram.toByteString)
      case x =>
        log.error(s"Unhandled message $x")
    }
  }
}

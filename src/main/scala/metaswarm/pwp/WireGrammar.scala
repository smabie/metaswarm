package metaswarm.pwp

import java.nio.ByteOrder

import akka.util.ByteString
import fastparse.byte.all._
import metaswarm.Config
import metaswarm.misc.Util._
import scalaz.{-\/, \/, \/-}

import scala.util.Try

/*
 * The Grammar for parsing the Peer Wire Protocol
 */
object WireGrammar {
  import metaswarm.pwp.WireDiagram._
  implicit val networkOrder = ByteOrder.BIG_ENDIAN

  implicit class withToBytes(x: Int) {
    val bs = ByteString.newBuilder.putInt(x).result().compact
    def toBytes = Bytes.view(bs.toArray)
    def toByteString = bs
  }

  val handShake: Parser[HandShake] = P(BS(Config.Plen)
    ~/ BS(Bytes.view(Config.Pstr.toArray))
    ~/ AnyBytes(8)    /* reserved */
    ~/ AnyBytes(20).! /* info_hash */
    ~/ AnyBytes(20).! /* peer id */
  ).map { case (infoHash, peerId) =>
      WireDiagram.HandShake(ByteString(infoHash.toArray), ByteString(peerId.toArray))
  }

  private[this] val keepAlive = P(BS(0.toBytes)).map(_ => KeepAlive)

  private[this] val choke = P(BS(1.toBytes) ~ BS(Choke.id)).map(_ => Choke)

  private[this] val unchoke = P(BS(1.toBytes) ~ BS(UnChoke.id)).map(_ => UnChoke)

  private[this] val interested = P(BS(1.toBytes) ~ BS(Interested.id)).map(_ => Interested)

  private[this] val notInterested = P(BS(1.toBytes) ~ BS(NotInterested.id)).map(_ => NotInterested)

  private[this] val have = P(BS(5.toBytes) ~ BS(4) ~ BE.Int32).map(Have)

  private[this] val bitfield = P(BE.Int32 ~ BS(5)).flatMap { x =>
    P(AnyBytes(x - 1).!).map { xs =>
      BitField(xs.toArray)
    }
  }

  private[this] val request = P(BS(13.toBytes) ~ BS(6) ~/ BE.Int32 ~/ BE.Int32 ~/ BE.Int32).map {
    case (index, begin, length) =>
      Request(index, begin, length)
  }

  private[this] val piece = P(BE.Int32 ~ BS(7) ~/ BE.Int32 ~/ BE.Int32).flatMap {
    case (len, index, begin) =>
      println(s"len: $len")
      P(AnyBytes(len - 9).!).map { block =>
        Piece(index, begin, ByteString(block.toArray))
      }
  }

  private[this] val wireDiagram =
    P(keepAlive
      | choke
      | unchoke
      | interested
      | notInterested
      | have
      | request
      | piece
      | bitfield)


  def parseHandshake(bs: ByteString): Option[HandShake] = {
    Try(handShake.parse(bs.toBytes).get.value).toOption
  }

  /*
   * Not used. Much slower that parseFast. If fastparse gets a speed boost, we
   * might go back to using it for general message parsing.
   */
  def parse(bs: ByteString): Int \/ (WireDiagram, Int) = {
    wireDiagram.parse(bs.toBytes).fold(
      { case (_, index, _) => -\/(index) },
      { case (value, index) => \/-(value -> index) })
  }


  /*
   * Very fast (and ugly) message parser. Returns either the parsed diagram and
   * the number of bytes read or true of false depending on if we encountered a
   * legitimate parsing error or if we just don't have enough data. True for a
   * parsing error and false for not enough data.
   */
  def parseFast(bs: ByteString): Boolean \/ (WireDiagram, Int) = {
    def getInt(bs: ByteString, index: Int): Int = {
      (((0xFF & bs(index)) << 24)
        | ((0xFF & bs(index + 1)) << 16)
        | ((0xFF & bs(index + 2)) << 8)
        | (0xFF & bs(index + 3)))
    }

    def slice(bs: ByteString, from: Int, until: Int): ByteString = {
      if (until > bs.length)
        throw new ArrayIndexOutOfBoundsException
      else
        bs.slice(from, until)
    }

    try {
      val x = getInt(bs, 0)
      if (x == 0) { /* KeepAlive */
        \/-(KeepAlive -> 1)
      } else if (x == 1) { /* Choke | UnChoke | Interested | NonInterested */
        val y = bs(4)
        if (y == 0)      \/-(Choke -> 5)
        else if (y == 1) \/-(UnChoke -> 5)
        else if (y == 2) \/-(Interested -> 5)
        else if (y == 3) \/-(NotInterested -> 5)
        else -\/(true)
      } else if (x == 5) { /* Have */
        if (bs(4) == 4) \/-(Have(getInt(bs, 5)) -> 9)
        else -\/(true)
      } else if (x == 13) { /* Request */
        if (bs(4) == 6) \/-(Request(getInt(bs, 5), getInt(bs, 9), getInt(bs, 13)) -> 17)
        else -\/(true)
      } else {
        val y = bs(4)
        if (y == 7) /* Piece */
          \/-(Piece(getInt(bs, 5), getInt(bs, 9), slice(bs, 13, x + 4)) -> (x + 4))
        else if (y == 5) /* BitField */
          \/-(BitField(slice(bs, 5, x + 4).toArray) -> (x + 4))
        else -\/(true)
      }
    } catch {
      /* If we get an exception, we don't have enough data */
      case _: Throwable => -\/(false)
    }
  }
}
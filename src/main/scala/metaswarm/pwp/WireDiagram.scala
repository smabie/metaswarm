package metaswarm.pwp

import akka.util.{ByteString, ByteStringBuilder}
import metaswarm.Config

object WireDiagram {
  import WireGrammar._

  def withLen(n: Int) = {
    ByteString.newBuilder.putInt(n)
  }

  implicit class RichByteStringBuilder(x: ByteStringBuilder) {
    def make = x.result.compact
  }

  sealed trait WireDiagram {
    def toByteString = ByteString.empty
    def id: Byte = 0
    def length: Int = 0
  }

  /* handshake: <pstrlen><pstr><reserved><info_hash><peer_id> */
  case class HandShake(infoHash: ByteString, peerId: ByteString = Config.PeerId) extends WireDiagram {
    private[this] val reserved = new Array[Byte](8)
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putByte(Config.Plen)
        .putBytes(Config.Pstr.toArray)
        .putBytes(reserved)
        .putBytes(infoHash.toArray)
        .putBytes(peerId.toArray)
        .result
        .compact
    }
    override def toString = "HandShake"
  }

  /* keep-alive: <len=0000> */
  case object KeepAlive extends WireDiagram {
    override lazy val toByteString: ByteString = length.toByteString
  }

  /* choke: <len=0001><id=0> */
  case object Choke extends WireDiagram {
    override lazy val toByteString: ByteString = ByteString.newBuilder.putInt(1).putByte(id).result.compact
    override def length = 1
  }

  /* unchoke: <len=0001><id=1> */
  case object UnChoke extends WireDiagram {
    override val id: Byte = 1
    override val length = 1
    override lazy val toByteString: ByteString = ByteString.newBuilder.putInt(length).putByte(id).result.compact
  }

  /* interested: <len=0001><id=2> */
  case object Interested extends WireDiagram {
    override val id: Byte = 2
    override val length = 1
    override lazy val toByteString: ByteString = ByteString.newBuilder.putInt(length).putByte(id).result.compact
  }

  /* not interested: <len=0001><id=3> */
  case object NotInterested extends WireDiagram {
    override val id: Byte = 3
    override val length = 1
    override lazy val toByteString: ByteString = ByteString.newBuilder.putInt(length).putByte(id).result.compact
  }

  /* have: <len=0005><id=4><piece index> */
  case class Have(piece: Int) extends WireDiagram {
    override val id: Byte = 4
    override val length = 5
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putInt(length)
        .putByte(id)
        .putInt(piece)
        .result
        .compact
    }
    override def toString = "Have"
  }


  /* bitfield: <len=0001+X><id=5><bitfield> */
  case class BitField(data: Array[Byte]) extends WireDiagram {
    override val id: Byte = 5
    override val length = data.length + 1
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putInt(length)
        .putByte(id)
        .putBytes(data)
        .result
        .compact
    }
    override def toString = "BitField"
  }

  /* request: <len=0013><id=6><index><begin><length> */
  case class Request(piece: Int, offset: Int, size: Int) extends WireDiagram {
    override val id: Byte = 6
    override val length = 13
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putInt(length)
        .putByte(id)
        .putInt(piece)
        .putInt(offset)
        .putInt(size)
        .result
        .compact
    }
    override def toString = "Request"
  }

 /* piece: <len=0009+X><id=7><index><begin><block> */
  case class Piece(piece: Int, offset: Int, block: ByteString) extends WireDiagram {
    override val id: Byte = 7
    override val length = 9 + block.length
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putInt(length)
        .putByte(id)
        .putInt(piece)
        .putInt(offset)
        .putBytes(block.toArray)
        .result
        .compact
    }
   override def toString = "Piece"
  }

  /* cancel: <len=0013><id=8><index><begin><length> */
  case class Cancel(piece: Int, offset: Int, size: Int) extends WireDiagram {
    override val id: Byte = 8
    override val length = 13
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putInt(length)
        .putByte(id)
        .putInt(piece)
        .putInt(offset)
        .putInt(size)
        .result
        .compact
    }
    override def toString = "Cancel"
  }

  /* port: <len=0003><id=9><listen-port> */
  case class Port(port: Int) extends WireDiagram {
    override val  id: Byte = 9
    override val length = 3
    override lazy val toByteString: ByteString = {
      ByteString.newBuilder
        .putInt(length)
        .putByte(id)
        .putInt(port)
        .result
        .compact
    }
  }
  override def toString = "Port"
}
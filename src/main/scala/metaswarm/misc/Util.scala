package metaswarm.misc

import java.net.InetSocketAddress

import akka.util.ByteString
import better.files.File
import fastparse.byte.all._
import metaswarm.metainfo.MetaInfo

import scala.collection.mutable.{Set => MSet}
import scala.collection.generic.{Growable, Shrinkable}
import scala.util.{Random, Try}


object Util {
  implicit class RichByteString(bs: ByteString) {
    private val bytes = Bytes.viewAt((idx: Long) => bs(idx.toInt), bs.size.toLong)
    def toHex: String = bytes.toHex
    def toBytes: Bytes = bytes
  }

  implicit class RichGrowShrink[-A](xs: Growable[A] with Shrinkable[A]) {
    def change(bool: Boolean, x: A): Unit = if (bool) xs += x else xs -= x
  }

  def ipToString(ip: Int) = s"${ip >> 24 & 0xff}.${ip >> 16 & 0xff}.${ip >> 8 & 0xff}.${ip & 0xff}"

  def allRW_?(files: Array[(File, Long)]) = {
    files.forall { case (fp, _) =>
      fp.exists && fp.isReadable && fp.isWriteable
    }
  }

  def actorName(name: String) = {
    val set = "-_.*$+:@&=,!~';.".toSet
    name.headOption.fold("") { ch =>
      (if (ch == '$') name.tail else name).filter { ch =>
        ch.isLetterOrDigit || set.contains(ch)
      }
    }
  }

  /* Generate random digits for PeerId */
  def genRandomClientId(len: Int) = {
    (1 to len).map { _ =>
      Random.nextInt(10).toString.charAt(0)
    }.mkString("")
  }

  def bytesRemaining(meta: MetaInfo, field: MutBitVector) = {
    meta.totalSize - field.highCount.toLong * meta.pieceSize.toLong
  }

  def formatMb(n: Long) = f"${n / (1024d * 1024d)}%1.2f MB"

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    println(s"Elapsed time: ${t1 - t0} ms")
    result
  }

  implicit class ToMutableSet[A](xs: Set[A]) {
    def toMutable = MSet.empty[A] ++ xs
  }
}


trait LocalRand {
  val Rand = new Random()
}

object TryOption {
  def apply[T](x: T) = Try(x).toOption
}

object SocketAddress {
  def apply(addr: String, port: Int) = new InetSocketAddress(addr, port)
}
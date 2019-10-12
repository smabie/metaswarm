package metaswarm.metainfo

import akka.util.ByteString
import fastparse.byte.all._
import metaswarm.misc.TryOption

/*
 * MetaInfoGrammar.parse parses a metainfo (torrent) bytestring and generates a
 * expression tree of all the bencoded data.
 */
object MetaInfoGrammar {
  import metaswarm.metainfo.Bencoder._

  import collection._
  implicit def BC(c: Char) = BS(c.toByte)

  private[this] val binDigits: Array[Byte] = "0123456789".map(_.toByte)(breakOut)

  private[this] val digits = P(BytesWhileIn(binDigits))

  /* ex: i32e */
  private[this] val benInt = P(Index ~ 'i' ~/ digits.! ~/ 'e' ~/ Index).map {
    case (start, x, end) =>
      x.decodeUtf8.fold(throw _, x => BenInt(x.toInt)(start, end - start))
  }

  /* ex: 3:foo */
  private[this] val benString = P(Index ~ digits.! ~/ ':').flatMap {
    case (start, bytes) =>
      val len = bytes.decodeUtf8.fold(throw _, _.toInt)
      P(AnyBytes(len).! ~ Index).map {
        case (x, end) =>
          BenString(ByteString.fromArrayUnsafe(x.toArray))(start, end - start)
      }
  }

  /* ex: l2:yo3:fooe */
  private[this] val benList = P(Index ~ 'l' ~/ benExp.rep(1) ~/ 'e' ~/ Index).map {
    case (start, x, end) =>
      BenList(x.toList)(start, end - start)
  }

  /* d3:foo2:yo3:bari20e */
  private[this] val benDict = P(Index ~ 'd' ~/ (benString ~ benExp).rep(1) ~/ 'e' ~/ Index).map {
    case (start, xs, end) =>
      BenDict(SortedMap.empty[BenString, BenExp] ++ xs)(start, end - start)
  }

  private val benExp: Parser[BenExp] = P(benList | benString | benInt | benDict)

  def parse(bytes: Array[Byte]): Option[BenExp] = {
    TryOption(MetaInfoGrammar.benExp.parse(Bytes.view(bytes)).get.value)
  }
}
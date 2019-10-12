package metaswarm.metainfo

import akka.util.ByteString
import metaswarm.misc.TryOption
import scalaz.syntax.std.option._

import scala.collection.SortedMap
import scala.util.Try

object Bencoder {
  implicit val benStringOrdering: Ordering[BenString] = Ordering.by(_.value.utf8String)

  /*
   * The BenExp is the data structure emitted from MetainfoGrammar.parse. It can
   * then be queried with apply and other methods. You probably shouldn't use
   * this data structure repeatedly as it is a little awkward.
   */
  sealed trait BenExp { self =>
    def value: Any
    def offset: Int
    def length: Int

    def apply(key: String): BenExp = get(key) err s"key '$key' not found"
    def get(key: String): Option[BenExp] = apply(ByteString(key))

    def apply(key: ByteString): Option[BenExp] = self match {
      case BenDict(value: SortedMap[BenString, BenExp]) => value.get(BenString(key)())
      case _ => None
    }

    def apply(elem: Int): Option[BenExp] = self match {
      case BenDict(value) => TryOption(value.slice(elem, elem + 1).head._2)
      case BenList(value) => value.lift(elem)
      case _ => None
    }

    def isString = false
    def isInt = false
    def isList = false
    def isDict = false

    def getStringByteVal: ByteString = sys.error("Trying to get invalid string")
    def getIntVal: Long = sys.error("Trying to get invalid int")
    def getListVal: List[BenExp] = sys.error("trying to get invalid list")
    def getDictVal: SortedMap[BenString, BenExp] = sys.error("trying to get invalid dict")
    def getStringVal: String = getStringByteVal.utf8String
  }

  /*
   * bencoded: 3:foo
   * decoded:  foo
   */
  case class BenString(value: ByteString)
                      (val offset: Int = 0, val length: Int = 0) extends BenExp {
    override def toString = s"BenString(${value.utf8String})"

    override def isString = true
    override def getStringByteVal = value
  }

  /*
   * bencoded: i32e
   * decoded:  32
   */
  case class BenInt(value: Long)
                   (val offset: Int = 0, val length: Int = 0) extends BenExp {

    override def isInt = true
    override def getIntVal = value
  }

  /*
   * bencoded: l3:fooi50e2:yoe
   * decoded:  [foo, 50, yo]
   */
    case class BenList(value: List[BenExp])
                      (val offset: Int = 0, val length: Int = 0) extends BenExp {

      override def isList = true
      override def getListVal = value
    }

  /*
   * bencoded: d4:spami32e3:foo2:hae
   * decoded:  [spam => 32, foo => ha]
   */
    case class BenDict(value: SortedMap[BenString, BenExp])
                      (val offset: Int = 0, val length: Int = 0) extends BenExp {

      override def isDict = true
      override def getDictVal = value
    }

  }



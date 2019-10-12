
import metaswarm.misc.MutBitVector
import org.scalameter.api._
import scodec.bits.BitVector
import org.scalameter.picklers.Implicits

object MBitVector extends Bench.LocalTime {
  override def persistor: Persistor = Persistor.None

  val size = 500000
  val unit = Gen.unit("size")
  performance of "Range" in {
  /*  measure method "map" in {
      using(ranges) in {
        r => r.map(_ + 1)
      }
    }*/
    measure method "MutBitVector" in {
      using(unit) in { r  =>

          val vec = MutBitVector(size)
          var i = 0
          while (i < size) {
            vec.high_!(i)
            i += 1
          }
      }
    }

    measure method "BitVector" in {
      using(unit) in { r =>
        var i = 0
        var vec = BitVector.low(size)
        while (i < size) {
            vec = vec.set(i)
            i += 1
          }
      }
    }
  }
}
package metaswarm.misc

object MutBitVector {
  def apply(length: Int) = new MutBitVector(length)
  def apply(array: Array[Byte], length: Int) = new MutBitVector(array, length)

  def empty: MutBitVector = apply(0)
}

/*
 * Mutable bit vector implementation. Used for both tracking partial pieces and
 * tracking bit_fields. A little bit ugly, but we need this to be as fast as
 * humanly possible.
 */
class MutBitVector(array: Array[Byte], val length: Int) extends Cloneable {

  def this(length: Int) = {
    this(new Array[Byte](math.ceil(length.toDouble / 8).toInt), length)
  }

  private[this] val NumBits = 8

  /* unsafe, take care! */
  private var buf = array

  /* the size of the underlying buffer */
  val size = buf.length

  private[this] def genArray(len: Int) = new Array[Byte](math.ceil(length.toDouble / NumBits).toInt)

  private[this] def boundsCheck_!(index: Int, length: Int): Unit = boundsCheck_!(index + length - 1)
  private[this] def boundsCheck_!(index: Int): Unit = {
    if (index >= length)
      throw new IndexOutOfBoundsException
  }

  private[this] def byteOffset(index: Int): Int = index / NumBits
  private[this] def bitOffset(index: Int): Int = 1 << ((NumBits - 1) - index % NumBits)

  private[this] def _low(index: Int) = {
    val byteIdx = byteOffset(index)
    val value = buf(byteIdx)

    buf(byteIdx) = (value & ~bitOffset(index)).toByte
  }

  private[this] def _high(index: Int): Unit = {
    val byteIdx = byteOffset(index)
    val value = buf(byteIdx)

    buf(byteIdx) = (value | bitOffset(index)).toByte
  }

  private[this] def _set(bool: Boolean, index: Int): Unit = {
    if (bool)
      _high(index)
    else
      _low(index)
  }

  private[this] def _get(index: Int): Boolean = {
    val value = buf(byteOffset(index))
    (value & bitOffset(index)) != 0
  }

  /* returns the value of the bit at the index */
  def apply(index: Int): Boolean = {
    boundsCheck_!(index)
    _get(index)
  }

  /* tests multiple bits starting at index */
  def apply(index: Int, length: Int): Boolean = {
    boundsCheck_!(index, length)
    var i = 0
    while (i < length && _get(index + i)) i += 1
    length == i
  }

  def high_!(index: Int): Unit = {
    boundsCheck_!(index)
    _high(index)
  }

  def high_!(index: Int, length: Int): Unit = {
    boundsCheck_!(index, length)
    var i = 0
    while (i < length) {
      _high(index + i)
      i += 1
    }
  }

  def low_!(index: Int): Unit = {
    boundsCheck_!(index)
    _low(index)
  }

  def low_!(index: Int, length: Int): Unit = {
    boundsCheck_!(index, length)
    var i = 0
    while (i < length) {
      _low(index + i)
      i += 1
    }
  }

  def set_!(bool: Boolean, index: Int): Unit = {
    boundsCheck_!(index)
    _set(bool, index)
  }
  def set_!(bool: Boolean, index: Int, length: Int) = {
    boundsCheck_!(index, length)
    var i = 0
    while (i < length) {
      _set(bool, index + i)
      i += 1
    }
  }

  def map_!(fn: Boolean => Boolean) = {
    var i = 0
    while (i < length) {
      _set(fn(_get(i)), i)
      i += 1
    }
  }

  def foreachWithIdx(fn: (Boolean, Int) => Unit) = {
    var i = 0
    while (i < length) {
      fn(_get(i), i)
      i += 1
    }
  }

  def allHigh_? : Boolean = apply(0, length)
  def clear() = buf = genArray(length)
  def toArray = buf.clone()
  def toUnsafeArray = buf

  def highCount: Int = {
    var count = 0
    var i = 0
    while (i < length) {
      if (_get(i))
        count += 1
      i += 1
    }
    count
  }

  def lowCount: Int = length - highCount

  /* Number of bits this vector has that the other one does not */
  def uniqueBits(other: MutBitVector): Int = {
    assert(this.length == other.length,
      "The length for both MutBitVectors in uniqueBits must be the same!")

    var i = 0
    var sum = 0
    while (i < length) {
      sum += Integer.bitCount(~buf(i) & other.buf(i))
      i += 1
    }
    sum
  }

  override def clone(): MutBitVector = MutBitVector(buf.clone(), length)
  override def toString: String = {
    val nb = StringBuilder.newBuilder
    foreachWithIdx { case (b, _) =>
      nb.append(if (b) 'X' else '0')
    }
    nb.toString()
  }
}
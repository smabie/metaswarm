package metaswarm.file

import java.io.InputStream

import better.files.File
import metaswarm.misc.TryOption
import metaswarm.misc.Util.allRW_?

import scala.annotation.tailrec

object FastFileReader {
  def apply(files: List[(File, Long)], size: Int) = new FastFileReader(files.toArray, size)
}

/*
 * Ugly but we want it to be as fast as possible.
 * Provides an abstraction so that we can read multiple files in a row from a single read call.
 * Also provides dummy data for gaps, missing files, and truncation for files that are too big
 */
class FastFileReader(files: Array[(File, Long)], size: Int) {
  assert(allRW_?(files), "All files must be readable and writeable!")

  private[this] val numFiles = files.length
  private[this] val buf = new Array[Byte](size)
  private[this] val (fps: Array[Option[InputStream]], totals: Array[Long]) = files.unzip match {
    case (f, lens) => f.map(x => TryOption(x.newInputStream)) -> lens
  }
  private[this] val incomplete = new Array[Boolean](numFiles)

  private[this] var index = 0

  private[this] def readCurrent(buf: Array[Byte], offset: Int, size: Int): Int = {
    val fp = files(index)._1
    val fileLen = files(index)._2

    /* remaining length of the file to be written */
    val rem = totals(index)

    fps(index) -> incomplete(index) match {
      /* we patch the missing or short file with dummy data */
      case (None, _) | (_, true) =>
        if (rem > size) {
        totals.update(index, rem - size)
        size
      } else rem.toInt

      case (Some(stream), _) =>
        val len = stream.read(buf, offset, size)

        /* We have an incomplete file */
        if ((len == -1 || len < size) && rem > size) {
          incomplete(index) =  true
          totals(index) = rem - size
          size
          /* The file is too long, so we truncate it */
        } else if (len > rem) {
          /* truncate the file to the correct size */
          fp.newFileOutputStream(append = true).getChannel.truncate(fileLen).close()
          rem.toInt
        }
        else {
          totals(index) =  rem - len
          len
        }
    }
  }

  /* Keep reading until we don't have any more data */
  @tailrec
  private[this] def recRead(buf: Array[Byte], offset: Int, size: Int): (Array[Byte], Int) = {
    val end_? = index == numFiles - 1
    val len = readCurrent(buf, offset, size)

    if (end_? && len == -1)
      Array.emptyByteArray -> 0
    else if ((end_? && len < size) || len == size)
      buf -> (offset + len)
    else {
      index += 1
      recRead(buf, offset + len, size - len)
    }
  }

  /* We aren't copying the array so we need to be careful */
  def read(): (Array[Byte], Int) = {
    recRead(buf, 0, size)
  }

  def close() = fps.foreach(_.foreach(_.close()))
}
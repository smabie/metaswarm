package metaswarm.actors.client.file

import java.security.MessageDigest

import akka.util.ByteString
import better.files.{File, _}
import metaswarm.metainfo.MetaInfo
import metaswarm.metainfo.MetaInfo.{ManyFiles, SingleFile}
import metaswarm.pwp.WireDiagram
import metaswarm.pwp.WireDiagram.Piece
import scalaz.{-\/, \/, \/-}
import xerial.larray.MappedLByteArray


object PieceMappedBuffer {
  def apply(meta: MetaInfo, path: String) = new PieceMappedBuffer(meta, path)
}


/*
 * Provides an interface over a collection of mmap'ed files so we can easily
 * read and write pieces. This isn't supposed to be elegant it's supposed to be
 * fast.
 */
class PieceMappedBuffer(meta: MetaInfo, path: String) {

  val files = meta.fileMeta match {
    case SingleFile(name, length) => path / name -> length :: Nil
    case ManyFiles(dir, files)    => files.map { case (name, length) => path / dir / name -> length }
  }

  /*
   * a collection of mmap'ed arrays or data for the missing file. To save space
   * we only allocate the space for the files when we have received the first
   * piece.
   */
  private[this] val buf: Array[\/[(File, Long), MappedLByteArray]] = files.map { case (f, len) =>
    if (f.exists)
      \/-(newMapped(f, len))
    else
      -\/(f -> len)
  }.toArray

  private[this] val fileSizes: Array[Long] = files.map(_._2).toArray

  /* cumulative array of files sizes. Used to generate the coordinates withe translate() */
  private[this] val totals: Array[Long] = files.map(_._2).scan(0.toLong)(_ + _).tail.toArray

  private[this] val digest = MessageDigest.getInstance("SHA-1")

  private[this] def newMapped(file: File, len: Long) = {
    new MappedLByteArray(file.createFileIfNotExists(true).toJava, offset = 0, size = len)
  }

  /*
   * Makes sure the file exists before we read/write from it. If we read from a
   * non-existent file, it will be created and 0s given back.
   */
  private[this] def populate_!(fileIdx: Int): MappedLByteArray = {
    buf(fileIdx) match {
      case -\/((f, len)) =>
        val mapped = newMapped(f, len)
        buf(fileIdx) = \/-(mapped)
        mapped
      case \/-(larray) => larray
    }
  }

  /*
   * Translates a piece index, offset and the data length to an array of
   * coordinates. Each coordinate specifies the file index, the offset in that
   * file and the data that should be written for the index and offset.
   */
  private[this] def translate(index: Int, offset: Int, len: Int): Array[(Int, Long, Long)] = {
    val blockOffset: Long = index.toLong * meta.pieceSize.toLong + offset.toLong

    val totalsWithIndex = totals.zipWithIndex

    val begin = totalsWithIndex.find(_._1 > blockOffset)
    val end = totalsWithIndex.find(_._1 > blockOffset + len - 1)

    begin -> end match {
      case (Some((_, beginIdx)), Some((_, endIdx))) =>
        val ret = new Array[(Int, Long, Long)](endIdx - beginIdx + 1)

        val fileOffset = blockOffset - (if (beginIdx == 0) 0 else totals(beginIdx - 1))
        val fileLen = fileSizes(beginIdx)

        /* the file spans the entire read/write */
        if (beginIdx == endIdx) {
          ret(0) = (beginIdx, fileOffset, len)
          ret

          /* two or more files span the entire read/write */
        } else {
          ret(0) = (beginIdx, fileOffset, fileLen - fileOffset)

          var length = len - (fileLen - fileOffset)
          var i = 1

          while (i < endIdx - beginIdx) {
            length -= fileSizes(beginIdx + i)
            ret(i) = (beginIdx + i, 0.toLong, fileSizes(beginIdx + i))
            i += 1
          }
          ret(i) = (beginIdx + i, 0.toLong, length)
          ret
        }

      case _ => Array.empty
    }

//    if (begin.isDefined && end.isDefined) {
//      val (beginIdx, endIdx) = begin.get._2 -> end.get._2
//
//      val ret = new Array[(Int, Long, Long)](endIdx - beginIdx + 1)
//
//      val fileOffset = blockOffset - (if (beginIdx == 0) 0 else totals(beginIdx - 1))
//      val fileLen = fileSizes(beginIdx)
//
//      /* the file spans the entire read/write */
//      if (beginIdx == endIdx) {
//        ret(0) = (beginIdx, fileOffset, len)
//        ret
//
//        /* two or more files span the entire read/write */
//      } else {
//        ret(0) = (beginIdx, fileOffset, fileLen - fileOffset)
//
//        var length = len - (fileLen - fileOffset)
//        var i = 1
//
//        while (i < endIdx - beginIdx) {
//          length -= fileSizes(beginIdx + i)
//          ret(i) = (beginIdx + i, 0.toLong, fileSizes(beginIdx + i))
//          i += 1
//        }
//        ret(i) = (beginIdx + i, 0.toLong, length)
//        ret
//      }
//    } else Array.empty
  }

  def verifyHash(index: Int, hash: ByteString): Boolean = {
    val size = meta.sizeOfPiece(index)
    read(index, 0, size).fold {
      sys.error(s"Could not find data for index: $index size: $size!")
    } { piece =>
      digest.update(piece.block.toArray)
      digest.digest() sameElements hash
    }
  }

  def write_!(piece: WireDiagram.Piece): Boolean = {
    val Piece(index, offset, block) = piece
    val coords = translate(index, offset, block.length)

    coords.length != 0 && {
      val arr: Array[Byte] = block.toArray

      var idx = 0
      var i = 0
      while (i < coords.length) {
        val (fileIdx, offset, len) = coords(i)
        idx += populate_!(fileIdx).readFromArray(arr, idx, offset, len.toInt)
        i += 1
      }
      true
    }
  }

  def read(index: Int, offset: Int, size: Int): Option[WireDiagram.Piece] = {
    val coords = translate(index, offset, size)

    if (coords.length == 0) None
    else {
      val arr = new Array[Byte](size)
      var idx = 0
      var i = 0

      while (i < coords.length) {
        val (fileIdx, offset, len) = coords(i)
        idx += populate_!(fileIdx).writeToArray(offset, arr, idx, len.toInt)
        i += 1
      }

      Some(WireDiagram.Piece(index, offset, ByteString(arr)))
    }
  }

  /* We need to call flush occasionally to make sure the data gets written */
  def flush() = {
    var i = 0
    while (i < buf.length) {
      buf(i).foreach(_.flush)
      i += 1
    }
  }

  def close() = {
    buf.foreach {
    case \/-(b) =>
      b.flush
      b.close()
    case -\/(_) =>
    }
  }
}
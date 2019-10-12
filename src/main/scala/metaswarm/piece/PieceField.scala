package metaswarm.piece

import java.security.MessageDigest

import akka.util.ByteString
import better.files._
import metaswarm.file.FastFileReader
import metaswarm.metainfo.MetaInfo
import metaswarm.metainfo.MetaInfo.{ManyFiles, SingleFile}
import metaswarm.misc.{MutBitVector, TryOption}

object PieceField {

  /*
   * Generates a BitVector that represents the file. We check the sha1 for
   * each piece to determine if we have it or still need to download it. We
   * only create a file when we've received at least one piece of it.
   */
  def load(meta: MetaInfo, path: String): Option[MutBitVector] = {
    val size = meta.pieceSize
    val pieceHashes = meta.pieces

    if (!File(path).isDirectory) {
      println("Path is not a directory")
      Option.empty[MutBitVector]
    } else meta.fileMeta match {
      case SingleFile(name, length) =>
        val file = path / name

        /* File does not exist so we just return the empty bitfield */
        if (file.notExists) {
          Some(MutBitVector(meta.numPieces))
        } else if (!file.isRegularFile) {
          println("File path is not a file")
          Option.empty[MutBitVector]
        } else {
          TryOption(calculate(List(file -> length), size, pieceHashes))
        }
      case ManyFiles(dir, files) =>
        TryOption(calculate(files.map(x => path / dir / x._1 -> x._2), size, pieceHashes))
    }
  }

  /* It's pretty disgusting how much faster this is than a grouped iterator */
  private[this] def calculate(files: List[(File, Long)], size: Int, hashes: Array[ByteString]): MutBitVector = {
    val digest = MessageDigest.getInstance("SHA-1")
    val ffr = FastFileReader(files, size)
    val len = hashes.length
    val pieceField = MutBitVector(len)

    var offset = 0
    while (offset < len) {
      val (readBuf, bufLen) = ffr.read()
      digest.update(readBuf, 0, bufLen)

      if (digest.digest() sameElements hashes(offset).toArray[Byte])
        pieceField.high_!(offset)

      offset += 1
    }
    pieceField
  }
}


package metaswarm.metainfo

import java.security.MessageDigest

import akka.util.ByteString
import better.files._
import metaswarm.metainfo.Bencoder._
import scalaz.syntax.std.option._

object MetaInfo {
  sealed abstract class FileInfo(val name: String) {
    val size: Long
  }

  case class SingleFile(override val name: String,
                        length: Long) extends FileInfo(name: String) {
    override val size = length
  }

  case class ManyFiles(override val name: String,
                       files: List[(String, Long)]) extends FileInfo(name: String) {
    override val size = files.map(_._2).sum
  }

  def apply(path: String): MetaInfo = {
    val file = File(path)
    val meta = MetaInfoGrammar.parse(file.loadBytes) err s"Can't parse $path, invalid torrent file"

    new MetaInfo(meta, file)
  }

  def getString(optExp: Option[BenExp]) = {
    optExp.fold("") { benExp =>
      if (benExp.isString)
        benExp.value.asInstanceOf[ByteString].utf8String
      else
        ""
    }
  }

  def genSha1Hash(file: File, offset: Int, length: Int) = {
    val buf = new Array[Byte](length)

    file.randomAccess().map { handle =>
      handle.seek(offset)
      handle.read(buf)
    }

    val digest: MessageDigest = MessageDigest.getInstance("SHA-1")
    digest.update(buf)
    ByteString(digest.digest())
  }

  /* If there's no announce-list, just use List(announce) */
  def genAnnounceList(exp: BenExp): List[List[String]] = {
    exp.get("announce-list").fold {
      val announce = exp("announce")
      List(List(announce.getStringVal))
    } { announceExp =>
      announceExp.getListVal.map { benExp =>
        benExp.getListVal.map(_.getStringVal)
      }
    }
  }

  /*
   * Generate the FileMeta from the parsed BenExp structure
   */
  def genMeta(info: BenExp): MetaInfo.FileInfo = {
    val fileExp = info.get("files")
    val name = info("name").getStringVal
    /* We know its only a single file */
    fileExp.fold {
      val length = info("length").getIntVal
      SingleFile(name, length): FileInfo
    } { listExp =>
      val fileMap: List[(String, Long)] = listExp.getListVal.map { dictExp =>
        val length = dictExp("length").getIntVal
        val path = dictExp("path").
                   getListVal.map(_.getStringVal).mkString("/")
        path -> length
      }(collection.breakOut)
      ManyFiles(name, fileMap)
    }
  }
}

/*
 * Stores all information read from the metainfo (torrent) file.
 */
class MetaInfo(root: BenExp, val file: File) {
  import MetaInfo._

  /* Make sure info and root can be GC'ed */
  val (infoHash: ByteString,
  pieceSize: Int,
  privTracker: Boolean,
  pieces: Array[ByteString],
  fileMeta: FileInfo) = {
    val info = root("info")
    val pieceExp = info("piece length")

    val infoHash: ByteString = genSha1Hash(file, info.offset, info.length)
    val pieceLength: Int = pieceExp.getIntVal.toInt
    val privTracker: Boolean = info.get("private").fold(false)(_.getIntVal == 1)
    val pieces = info("pieces").getStringByteVal.grouped(20).toArray
    val fileMeta = genMeta(info)

    (infoHash, pieceLength, privTracker, pieces, fileMeta)
  }

  val totalSize = fileMeta.size
  val lastPieceSize = (totalSize % pieceSize).toInt
  val numPieces = pieces.length

  val announceList: List[List[String]] = genAnnounceList(root)

  val comment = getString(root.get("comment"))
  val creationDate = getString(root.get("creation date"))
  val createdBy = getString(root.get("created by"))
  val encoding = getString(root.get("encoding"))

  val name = fileMeta.name

  def sizeOfPiece(index: Int) = {
    if (index == numPieces - 1)
      lastPieceSize
    else pieceSize
  }
}

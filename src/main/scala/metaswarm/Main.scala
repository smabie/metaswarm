package metaswarm

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import better.files._
import com.typesafe.config.ConfigFactory
import metaswarm.actors.client.{ListenerSupervisor, Torrent}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


object DumbActor {
  def apply()(implicit system: ActorSystem) = system.actorOf(Props(new DumbActor))
}

class DumbActor extends Actor {
  def receive: Receive = {
    case _ => println("dumb")
  }
}

object Main {

  def main(args: Array[String]): Unit = {
      val conf = ConfigFactory.load(ConfigFactory.parseFile(File("/home/sturm/akbt/application.conf").toJava))
  /*val backup = ConfigFactory.load();
  val combined = conf.withFallback(backup)
  val complete = ConfigFactory.load(combined)*/


  implicit val system: ActorSystem = ActorSystem("tracker", conf)

  val path = "/home/sturm/misc/test.mkv.torrent"
  val path2 = "/home/sturm/misc/test2.mkv.torrent"

  val supervisor = system.actorOf(Props(new ListenerSupervisor()), "listener-supervisor")

  supervisor ! ListenerSupervisor.Start

  implicit val timeout = Timeout(5 seconds)
  for {
    (trackerListener: ActorRef, peerListener: ActorRef) <- supervisor ? ListenerSupervisor.GetListenerRefs
  } yield {
    system.actorOf(Props(new Torrent(torrentPath = path2, filePath = "/home/sturm/tmp/", trackerListener,
      peerListener)), "test2") ! Torrent.Start
    system.actorOf(Props(new Torrent(torrentPath = path, filePath = "/home/sturm/tmp/", trackerListener,
     peerListener)), "test") ! Torrent.Start

  }

  while(true) {
    Thread.sleep(500)
  }

  /*val foo = MetaInfo("/home/sturm/ttest/venom.torrent")
  val bar = PieceField.load(foo,"/home/sturm/ttest/" )*/


  /*val vec = MutBitVector(4)

  val p1 = MutBitVector(4)
  val p2 = MutBitVector(4)
  val p3 = MutBitVector(4)

  p1.high(0)

  p2.high(1)

  p3.high(2)
  p3.high(1)

  println(vec)

  val pp = new PiecePicker(vec, 2 * Config.RequestSize)

  val da1 = DumbActor()
  val da2 = DumbActor()
  val da3 = DumbActor()

  pp.addPeer(da1, p1)
  pp.addPeer(da2, p2)

  println(pp)


  pp.addBlock(0, Config.RequestSize)

  println(pp)
  println(pp.pieceQueue.head)
  mutable.WrappedArray*/
  /*pp.addPeer(da2, p2)
  pp.addPeer(da3, p3)

  println(pp)

  pp.addBlock(0, 0)
  pp.addPeerPiece(da1, WireDiagram.Have(0))

  println(pp)

  pp.addBlock(0, Config.RequestSize)

  println(pp)

  /*

  val a = time(PieceField(meta, "/home/sturm/test2/"))*/

  //val vec = Vector((file"/", 5.toLong), (file"/", 3.toLong), (file"/", 2.toLong))
  // 01234 567 89
  // 01234 012 01

  //val ffw = FastFileWriter(vec, 2)

  // 01234 567 89
  // 01234 012 01
  //println(ffw.translate(0, 1, 2)) // (1, 1, 2)
  //println(ffw.translate(1, 1, 6))

  /*val pb = new PieceMappedBuffer(List((file"/home/sturm/buf/test", 10)), 3)

  val piece = WireDiagram.Piece(0, 0, ByteString("hello"))
  val piece2 = WireDiagram.Piece(0, 1, ByteString("hello"))
  val piece3 = WireDiagram.Piece(1, 0, ByteString("hello"))
  pb.write(piece)
  pb.write(piece2)
  pb.write(piece3)


  pb.close()*/
  /*val ffr = FastFileReader(List(file"/home/sturm/test2/test/foo",file"/home/sturm/test2/test/bar"), 3)


  var (arr, len) = ffr.read()
  println(new String(arr))

  while (len != 0) {
    ffr.read() match {
      case (buf, size) =>
        arr = buf
        len = size
        println(s"${new String(arr.slice(0, len))} size: $len")
    }
  }*/*/
  }
}
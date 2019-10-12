package metaswarm

import akka.util.ByteString
import com.github.nscala_time.time.Imports._
import metaswarm.misc.Util

object Config {
  val Version = "0100" /* 0.1.0.0 */
  val ClientId = "MS"
  val PeerId = ByteString(s"-$ClientId$Version-${Util.genRandomClientId(12)}")

  val PeerTimeout = 2 minutes
  val TimeoutKey = "timeout"

  /* miss / (hit + miss) */
  val LinearFillCost = 0.8
  val LinearFillReduceRatio = 0.5
  val LinearFillMissPenalty = 10

  /* Factor on total number of peers */
  val MinLinearSearch = 1.0




  val InitialPeerBufferSize = 15
  val PeerBufferStep = 10

  val MaxConnectedPeers = 50
  val MaxActivePeers = 30
  val PeerReplacement = 5
  val KeepAmount = MaxActivePeers - PeerReplacement

  val RequestSize = math.pow(2, 14).toInt

  val Pstr = ByteString("BitTorrent protocol".map(_.toByte).toArray)
  val Plen: Byte = Pstr.length.toByte

  val TrackerUdpPort = 35000
  val PeerTcpPort = 6969

  val RankTime = 10 seconds
  val LocalAddress = "0.0.0.0"

  val Scorer =  "peer-score-download"
  val Strategy = "linear-fill"

  val StaleRequestTime = (1 minute).toDuration
}


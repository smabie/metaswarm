package metaswarm.piece

import akka.actor.ActorRef
import com.github.nscala_time.time.Imports._
import metaswarm.Config
import metaswarm.pwp.WireDiagram
import scalaz.syntax.std.option._

import scala.collection.mutable.{Map => MMap, Set => MSet, TreeMap => MTreeMap}

object SentRequests {
  def apply() = new SentRequests()
}

class SentRequests {
  private[this] val actorToReqs = MMap.empty[ActorRef, MSet[WireDiagram.Request]]
  private[this] val reqsToActor = MMap.empty[WireDiagram.Request, ActorRef]
  private[this] val reqsToDt = MMap.empty[WireDiagram.Request, DateTime]
  private[this] val dtToReqs = MTreeMap.empty[DateTime, MSet[WireDiagram.Request]](Ordering.by(_.getMillis))

  def containsRequest(req: WireDiagram.Request) = reqsToActor.contains(req)

  def actorRequests(actor: ActorRef) = actorToReqs(actor).toList

  def addRequest_!(req: WireDiagram.Request, now: DateTime, actor: ActorRef): Unit = {
    reqsToDt(req) = now
    reqsToActor(req) = actor

    if (actorToReqs.contains(actor))
      actorToReqs(actor) += req
    else
      actorToReqs(actor) = MSet(req)

    if (dtToReqs.contains(now))
      dtToReqs(now) += req
    else
      dtToReqs(now) = MSet(req)
  }

  def removeRequest_!(req: WireDiagram.Request): Unit = {
    val dt = reqsToDt(req)

    val dtSet = dtToReqs(dt)
    if (dtSet.size == 1)
      dtToReqs -= dt
    else
      dtSet -= req

    val actor =  reqsToActor(req)
    val peerSet = actorToReqs(actor)
    if (peerSet.size == 1)
      actorToReqs -= actor
    else
      peerSet -= req

    reqsToActor -= req
    reqsToDt -= req
  }

  def removeStale_!(now: DateTime): List[(WireDiagram.Request, ActorRef)] = {
    dtToReqs.takeWhile { case (dt, _) =>
      (dt to now).toDuration >= Config.StaleRequestTime
    }.toList.flatMap { case (dt, reqs) =>
      dtToReqs -= dt
      reqs.map { req =>
        reqsToDt -= req
        val actor = reqsToActor.remove(req) err "Can't find request!"
        actorToReqs -= actor
        req -> actor
      }.toList
    }
  }
}
/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */
package com.bwater.notebook.kernel.remote

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.commons.io.FileUtils

import com.bwater.notebook.kernel.pfork.BetterFork
import com.bwater.notebook.kernel.pfork.ForkableProcess
import com.bwater.notebook.kernel.pfork.ProcessInfo
import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.actor.AddressFromURIString
import akka.actor.Deploy
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionKey
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.remote.RemoteActorRefProvider
import akka.remote.RemoteScope

class RemoteActorProcess extends ForkableProcess {
  // http://stackoverflow.com/questions/14995834/programmatically-obtain-ephemeral-port-with-akka
  var _system: ActorSystem = null

  def init(args: Seq[String]): String = {
    val configFile = args(0)
    val cfg = ConfigFactory.load(configFile)

    // Cookie file is optional second argument
    val actualCfg = args match {
      case Seq(_, cookieFile) if (cookieFile.size > 0) =>
        val cookie = FileUtils.readFileToString(new File(cookieFile))
        AkkaConfigUtils.requireCookie(cfg, cookie)
      case _ => cfg
    }

    _system = ActorSystem("Remote", actualCfg)

    val address = GetAddress(_system).address
    address.toString
    // address.port.getOrElse(sys.error("not a remote actor system: %s".format(cfg))).toString
  }

  def waitForExit() {
    _system.awaitTermination()
    println("waitForExit complete")
  }
}

// —————————————————————————————————————————————————————————————————————————————————————————————————

class FindAddressImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider match {
    case rarp: RemoteActorRefProvider => rarp.transport.address
    case _ => system.provider.rootPath.address
  }
}

object GetAddress extends ExtensionKey[FindAddressImpl]
case object RemoteShutdown

// —————————————————————————————————————————————————————————————————————————————————————————————————

class ShutdownActor extends Actor {
  override def postStop() {
    // KV: I tried to do a context.system.shutdown() here, but the system would often hang when multiple actors were in play.
    //  I think it was this issue: https://groups.google.com/forum/#!msg/akka-user/VmKMPI_tNQU/ZUSz25OBpIwJ
    // So we take the hard way out. Would be nice to have graceful shutdown
    sys.exit(0)
  }

  def receive = Map.empty
}

// —————————————————————————————————————————————————————————————————————————————————————————————————

/**
 * Represents a running remote actor system, with an address and the ability to kill it
 */
class RemoteActorSystem(localSystem: ActorSystem, info: ProcessInfo, remoteContext: ActorRefFactory) {
  def this(localSystem: ActorSystem, info: ProcessInfo) = this(localSystem, info, localSystem)

  val address = AddressFromURIString(info.initReturn)

  val shutdownActor = remoteContext.actorOf(Props(new ShutdownActor).withDeploy(Deploy(scope = RemoteScope(address))))

  def deploy = Deploy(scope = RemoteScope(address))

  def actorOf(context: ActorRefFactory, props: Props) = context.actorOf(props.withDeploy(deploy))

  def shutdownRemote() { shutdownActor ! PoisonPill }
  def killRemote() { info.kill() }

}

// —————————————————————————————————————————————————————————————————————————————————————————————————

/**
 * Create a remote actor system
 */
object RemoteActorSystem {
  def spawn(system: ActorSystem, configFile: String): Future[RemoteActorSystem] = {
    val cookiePath = AkkaConfigUtils.requiredCookie(system.settings.config) match {
      case Some(cookie) =>
        val cookieFile = new File(".", ".akka-cookie")
        FileUtils.writeStringToFile(cookieFile, cookie)
        cookieFile.getAbsolutePath
      case _ => ""
    }
    new BetterFork[RemoteActorProcess](system.dispatcher)
        .execute(configFile, cookiePath)
        .map { new RemoteActorSystem(system, _) }
  }
}

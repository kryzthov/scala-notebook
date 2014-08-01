package akka.remote

import akka.actor.ExtensionKey
import akka.actor.ExtendedActorSystem
import akka.actor.Extension

class FindAddressImpl(system: ExtendedActorSystem) extends Extension {
  def address = system.provider match {
    case rarp: RemoteActorRefProvider => rarp.transport.defaultAddress
    case _ => system.provider.rootPath.address
  }
}

object GetAddress extends ExtensionKey[FindAddressImpl]
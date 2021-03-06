/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */

package com.bwater.notebook.kernel.remote

import scala.collection.JavaConversions.mapAsJavaMap

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 *
 */

object AkkaConfigUtils {

  private val SecureCookiePath = "akka.remote.netty.secure-cookie"
  private val RequireCookiePath = "akka.remote.netty.require-cookie"

  /** Creates a configuration that requires the specified secure requiredCookie if defined. */
  def requireCookie(baseConfig: Config, cookie: String) =
    ConfigFactory.parseMap(Map(
      SecureCookiePath -> cookie,
      RequireCookiePath -> "on"
    )).withFallback(baseConfig)

  /**
   * If the specified configuration requires a secure requiredCookie,
   * but does not define the requiredCookie value,
   * this generates a new config with an appropriate value.
   */
  def optSecureCookie(baseConfig: Config, cookie: => String): Config = {
    requiredCookie(baseConfig).map {
      req => if (req.isEmpty) {
        ConfigFactory.parseMap(Map(SecureCookiePath -> cookie)).withFallback(baseConfig)
      } else baseConfig
    } getOrElse baseConfig
  }

  /** Returns the secure requiredCookie value if the specified Config requires their use. */
  def requiredCookie(config: Config): Option[String] =
    if (config.getBoolean(RequireCookiePath)) {
      if (config.hasPath(SecureCookiePath))
        Some(config.getString(SecureCookiePath))
      else
        Some("")
    } else {
      None
    }
}

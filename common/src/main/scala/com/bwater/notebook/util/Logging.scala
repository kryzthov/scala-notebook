/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */

package com.bwater.notebook.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ScalaLogger private (
  private final val log: Logger
) {

  def error(format: String, args: Any*): Unit = {
    val parameters: Array[Any] = args.toArray[Any]
    log.error(format, parameters)
  }

  def warn(format: String, args: Any*): Unit = {
    val parameters: Array[Any] = args.toArray[Any]
    log.warn(format, parameters)
  }

  def info(format: String, args: Any*): Unit = {
    val parameters: Array[Any] = args.toArray[Any]
    log.info(format, parameters)
  }

  def debug(format: String, args: Any*): Unit = {
    val parameters: Array[Any] = args.toArray[Any]
    log.debug(format, parameters)
  }

  def trace(format: String, args: Any*): Unit = {
    val parameters: Array[Any] = args.toArray[Any]
    log.trace(format, parameters)
  }
}

object ScalaLogger {
  def apply(clazz: Class[_]): ScalaLogger = {
    new ScalaLogger(LoggerFactory.getLogger(clazz))
  }
}

// -------------------------------------------------------------------------------------------------

trait Logging {
  private[this] val log = LoggerFactory.getLogger(this.getClass)
  protected[this] final val LOG = ScalaLogger(this.getClass)

  protected[this] def logInfo(messageGenerator: => String): Unit = {
    if (log.isInfoEnabled) log.info(messageGenerator)
  }

  protected[this] def logInfo(messageGenerator: => String, e: Throwable): Unit = {
    if (log.isInfoEnabled) log.info(messageGenerator, e)
  }

  protected[this] def logDebug(messageGenerator: => String): Unit = {
    if (log.isDebugEnabled) log.debug(messageGenerator)
  }

  protected[this] def logDebug(messageGenerator: => String, e: Throwable): Unit = {
    if (log.isDebugEnabled) log.debug(messageGenerator, e)
  }

  protected[this] def logTrace(messageGenerator: => String): Unit = {
    if (log.isTraceEnabled) log.trace(messageGenerator)
  }

  protected[this] def logTrace(messageGenerator: => String, e: Throwable): Unit = {
    if (log.isTraceEnabled) log.info(messageGenerator, e)
  }

  protected[this] def logError(messageGenerator: => String): Unit = {
    log.error(messageGenerator)
  }

  protected[this] def logError(messageGenerator: => String, e: Throwable): Unit = {
    log.error(messageGenerator, e)
  }

  protected[this] def logWarn(messageGenerator: => String): Unit = {
    log.warn(messageGenerator)
  }

  protected[this] def logWarn(messageGenerator: => String, e: Throwable): Unit = {
    log.warn(messageGenerator, e)
  }

  protected[this] def ifErrorEnabled(f: => Unit): Unit = {
    if (log.isErrorEnabled()) {
      f
    }
  }

  protected[this] def ifWarnEnabled(f: => Unit): Unit = {
    if (log.isWarnEnabled()) {
      f
    }
  }

  protected[this] def ifInfoEnabled(f: => Unit): Unit = {
    if (log.isInfoEnabled()) {
      f
    }
  }

  protected[this] def ifDebugEnabled(f: => Unit): Unit = {
    if (log.isDebugEnabled()) {
      f
    }
  }

  protected[this] def ifTraceEnabled(f: => Unit): Unit = {
    if (log.isTraceEnabled()) {
      f
    }
  }
}
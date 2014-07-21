package scala.tools.nsc.interpreter

import scala.tools.nsc.Settings

/**
 * Subclass to access some hidden things I need and also some custom behavior.
 */
class HackIMain(
    settings: Settings,
    out: JPrintWriter
) extends IMain(settings, out) {
  def previousRequests: List[Request] = prevRequestList
}

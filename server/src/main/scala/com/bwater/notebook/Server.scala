package com.bwater.notebook

import java.io.File
import java.io.IOException
import java.net.InetAddress
import java.net.URLEncoder

import org.apache.commons.io.FileUtils
import org.apache.log4j.PropertyConfigurator
import org.jboss.netty.handler.stream.ChunkedWriteHandler

import com.bwater.notebook.server.ClientAuth
import com.bwater.notebook.server.Dispatcher
import com.bwater.notebook.server.DispatcherSecurity
import com.bwater.notebook.server.Insecure
import com.bwater.notebook.server.ScalaNotebookConfig
import com.bwater.notebook.util.Logging

import unfiltered.netty.Http
import unfiltered.netty.Resources
import unfiltered.netty._

/**embedded server */
object Server extends Logging {

  FileUtils.forceMkdir(new File("logs"))

  def openBrowser(url: String) {
    logInfo("Opening URL %s".format(url))
    unfiltered.util.Browser.open(url) match {
      case Some(ex) => logError("Error opening URL %s in browser:\n%s".format(url, ex.toString))
      case None => ()
    }
  }

  /**
   * Scala Notebook server entry point.
   */
  def main(args: Array[String]) {
    startServer(args, ScalaNotebookConfig.withOverrides(ScalaNotebookConfig.defaults))(openBrowser)
  }

  private val DefaultPreferredPort = 8899

  // This is basically unfiltered.util.Port.any with a preferred port, and is host-aware. Like the original, this
  // approach can be really unlucky and have someone else steal our port between opening this socket and when unfiltered
  // opens it again, but oh well...
  private def choosePort(host: String, preferredPort: Int) = {
    val addr = InetAddress.getByName(host)

    // 50 for the queue size is java's magic number, not mine. The more common ServerSocket constructor just
    // specifies it for you, and we need to pass in addr so we pass in the magic number too.
    val s = try {
      new java.net.ServerSocket(preferredPort, 50, addr)
    } catch {
      case ex: IOException =>
        new java.net.ServerSocket(0, 50, addr)
    }
    val p = s.getLocalPort
    s.close()
    p
  }

  def startServer(args: Array[String], config: ScalaNotebookConfig)(startAction: (String) => Unit) {
    PropertyConfigurator.configure(getClass.getResource("/log4j.server.properties"))
    val classpath = System.getProperty("java.class.path").split(":")
    logDebug("ScalaNotebook server classpath:\n%s".format(classpath.map { cp => "\t" + cp }.mkString("\n")))

    val secure = !args.contains("--disable_security")

    logInfo("Running SN Server in " + config.notebooksDir.getAbsolutePath)
    val NotebookArg = "--notebook=(\\S+)".r
    val notebook = args.collect { case NotebookArg(name) => name }.headOption

    val HostArg = "--host=(\\S+)".r
    val host = args.collect { case HostArg(name) => name }.headOption.getOrElse("localhost")

    val PortArg = "--port=(\\S+)".r
    val preferredPort = args.collect { case PortArg(name) => name }.headOption.map{_.toInt}.getOrElse(DefaultPreferredPort)

    logInfo("Starting server on %s:%s".format(host, preferredPort))

    val port = choosePort(host, preferredPort)
    if (preferredPort != DefaultPreferredPort) {
      require(port == preferredPort)
    }

    val security = if (secure) new ClientAuth(host, port) else Insecure

    val queryString =
      for (name <- notebook)
      yield "?dest=" + URLEncoder.encode("/view/" + name, "UTF-8")

    startServer(config, host, port, security) {
      val baseUrl = "http://%s:%d/%s".format(host, port, security.loginPath)
      (http, app) => startAction((baseUrl ++ queryString).mkString)
    }
  }

  /* TODO: move host, port, security settings into config? */
  def startServer(
      config: ScalaNotebookConfig,
      host: String,
      port: Int,
      security: DispatcherSecurity
  )(
      startAction: (Http, Dispatcher) => Unit
  ) {
    if (!config.notebooksDir.exists()) {
      logWarn(
          ("Base directory %s for Scala Notebook server does not exist. "
          + "Creating, but your server may be misconfigured.").format(config.notebooksDir))
      config.notebooksDir.mkdirs()
    }

    val app: Dispatcher = new Dispatcher(config, host, port)
    import security.{ withCSRFKey, withCSRFKeyAsync, withWSAuth, authIntent }

    val wsPlan = unfiltered.netty.websockets.Planify(withWSAuth(app.WebSockets.intent)).onPass(_.sendUpstream(_))

    val authPlan = unfiltered.netty.cycle.Planify(authIntent)

    val nbReadPlan = unfiltered.netty.cycle.Planify(withCSRFKey(app.WebServer.nbReadIntent))
    val nbWritePlan = unfiltered.netty.cycle.Planify(withCSRFKey(app.WebServer.nbWriteIntent))
    val templatesPlan = unfiltered.netty.cycle.Planify(app.WebServer.otherIntent)
    val kernelPlan = unfiltered.netty.async.Planify(withCSRFKeyAsync(app.WebServer.kernelIntent))
    val loggerPlan = unfiltered.netty.cycle.Planify(new ReqLogger().intent)
    val obsInt = unfiltered.netty.websockets.Planify(withWSAuth(new ObservableIntent(app.system).webSocketIntent))
        .onPass(_.sendUpstream(_))

    val iPythonRes = Resources(getClass.getResource("/from_ipython/"), 3600, true)
    val thirdPartyRes = Resources(getClass.getResource("/thirdparty/"), 3600, true)

    // TODO: absolute URL's may not be portable, should they be supported?
    // If not, are resources defined relative to notebooks dir or module root?
    def userResourceURL(res: File) = {
      if (res.isAbsolute()) res.toURI().toURL()
      else new File(config.notebooksDir, res.getPath()).toURI().toURL()
    }
    val moduleRes = config.serverResources map (res => Resources(userResourceURL(res), 3600, true))
    val observableRes = Resources(getClass.getResource("/observable/"), 3600, false)

    val http = unfiltered.netty.Http(port, host)

    class Pipe[A](value: A) {
      def pipe[B](f: A => B): B = f(value)
    }
    implicit def Pipe[A](value: A) = new Pipe(value)

    def resourcePlan(res: Resources*)(h: Http) =
        res.foldLeft(h)((h, r) => h.plan(r).makePlan(new ChunkedWriteHandler))

    http
      .handler(obsInt)
      .handler(wsPlan)
      .chunked(256 << 20)
      .handler(loggerPlan)

      .handler(authPlan)

      .handler(nbReadPlan)
      .handler(nbWritePlan)
      .handler(kernelPlan)
      .handler(templatesPlan)

      /* Workaround for https://github.com/unfiltered/unfiltered/issues/139 */
      .pipe(resourcePlan(iPythonRes, thirdPartyRes))
      .pipe(resourcePlan(moduleRes: _*))
      .pipe(resourcePlan(observableRes))
      .run({
        svr =>
          startAction(svr, app)
      }, {
        svr =>
          logInfo("shutting down server")
          KernelManager.shutdown()
          app.system.shutdown()
      })
  }
}

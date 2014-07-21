/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */

package com.bwater.notebook.kernel

import java.io.ByteArrayOutputStream
import java.io.PrintWriter
import java.io.StringWriter
import java.util.ArrayList
import scala.collection.mutable
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.tools.jline.console.completer.ArgumentCompleter
import scala.tools.jline.console.completer.Completer
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Completion.Candidates
import scala.tools.nsc.interpreter.Completion.ScalaCompleter
import scala.tools.nsc.interpreter.HackIMain
import scala.tools.nsc.interpreter.JLineCompletion
import scala.tools.nsc.interpreter.JLineDelimiter
import scala.tools.nsc.interpreter.JList
import scala.tools.nsc.interpreter.Parsed
import scala.tools.nsc.interpreter.Results.Error
import scala.tools.nsc.interpreter.Results.{Incomplete => ReplIncomplete}
import scala.tools.nsc.interpreter.Results.{Success => ReplSuccess}
import scala.util.control.NonFatal
import scala.xml.NodeSeq
import scala.xml.Text
import com.bwater.notebook.Match
import com.bwater.notebook.Widget
import com.bwater.notebook.util.Logging
import scala.reflect.io.VirtualDirectory

/**
 * This is what actually executes REPL statements, relying on a hacked IMain instance.
 */
final class Repl(compilerOpts: List[String]) extends Logging {
  import Repl._

  LOG.info("Initializing new REPL instance: {}", this)
  instances += this

  def this() = this(Nil)

  type OutputProcessor = (String => Unit)
  private final val NullOutputProcessor: OutputProcessor = { output: String => () }

  class MyOutputStream extends ByteArrayOutputStream {
    var aop: OutputProcessor = NullOutputProcessor

    override def write(i: Int): Unit = {
      // CY: Not used...
      //      orig.value ! StreamResponse(i.toString, "stdout")
      super.write(i)
    }

    override def write(bytes: Array[Byte]): Unit = {
      // CY: Not used...
      //      orig.value ! StreamResponse(bytes.toString, "stdout")
      super.write(bytes)
    }

    override def write(bytes: Array[Byte], off: Int, length: Int): Unit = {
      val data = new String(bytes, off, length)
      aop(data)
      //      orig.value ! StreamResponse(data, "stdout")
      super.write(bytes, off, length)
    }
  }


  private val stdoutBytes = new MyOutputStream
  private val stdout = new PrintWriter(stdoutBytes)

  private val interp: HackIMain = {
    val settings = new Settings
    settings.embeddedDefaults[Repl]
    if (!compilerOpts.isEmpty)
      settings.processArguments(compilerOpts, false)

    // TODO: This causes tests to fail in SBT, but work in IntelliJ
    // The java CP in SBT contains only a few SBT entries (no project entries), while
    // in intellij it has the full module classpath + some intellij stuff.
    settings.usejavacp.value = true
    // println(System.getProperty("java.class.path"))
    val imain = new HackIMain(settings, stdout)
    imain.initializeSynchronous()
    imain
  }

  private val completion = new JLineCompletion(interp)

  private def scalaToJline(tc: ScalaCompleter): Completer = new Completer {
    def complete(_buf: String, cursor: Int, candidates: JList[CharSequence]): Int = {
      val buf   = if (_buf == null) "" else _buf
      val Candidates(newCursor, newCandidates) = tc.complete(buf, cursor)
      newCandidates foreach (candidates add _)
      newCursor
    }
  }

  private val argCompletor = {
    val arg = new ArgumentCompleter(new JLineDelimiter, scalaToJline(completion.completer()))
    // turns out this is super important a line
    arg.setStrict(false)
    arg
  }

  private val stringCompletor = StringCompletorResolver.completor

  private def getCompletions(line: String, cursorPosition: Int) = {
    val candidates = new ArrayList[CharSequence]()
    argCompletor.complete(line, cursorPosition, candidates)
    candidates.asScala map { _.toString } toList
  }

  /**
   * Evaluates the given code.  Swaps out the `println` OutputStream with a version that
   * invokes the given `onPrintln` callback everytime the given code somehow invokes a
   * `println`.
   *
   * Uses compile-time implicits to choose a renderer.  If a renderer cannot be found,
   * then just uses `toString` on result.
   *
   * I don't think this is thread-safe (largely because I don't think the underlying
   * IMain is thread-safe), it certainly isn't designed that way.
   *
   * @param code
   * @param onPrintln
   * @return result and a copy of the stdout buffer during the duration of the execution
   */
  def evaluate(
      code: String,
      onPrintln: OutputProcessor = NullOutputProcessor
  ): (EvaluationResult, String) = {
    stdout.flush()
    stdoutBytes.reset()

    // capture stdout if the code the user wrote was a println, for example
    stdoutBytes.aop = onPrintln
    val res = Console.withOut(stdoutBytes) {
      interp.interpret(code)
    }
    stdout.flush()
    stdoutBytes.aop = NullOutputProcessor

    val result = res match {
      case ReplSuccess =>
        val request = interp.previousRequests.last
        val lastHandler: interp.memberHandlers.MemberHandler = request.handlers.last

        try {
          val evalValue = if (lastHandler.definesValue) {
            // This is true for def's with no parameters,
            // not sure that executing/outputting this is desirable
            // CY: So for whatever reason, line.evalValue attemps to call the $eval method
            // on the class...a method that does not exist. Not sure if this is a bug in the
            // REPL or some artifact of how we are calling it.
            // RH: The above comment may be going stale given the shenanigans I'm pulling below.
            val line = request.lineRep
            val renderObjectCode =
              """object $rendered {
                |  %s
                |  val rendered: _root_.com.bwater.notebook.Widget = %s
                |  %s
                |}""".stripMargin.format(request.importsPreamble, request.fullPath(lastHandler.definesTerm.get), request.importsTrailer)
            if (line.compile(renderObjectCode)) {
              val renderedClass = Class.forName(line.pathTo("$rendered") + request.accessPath.replace('.', '$') + "$", true, interp.classLoader)
              renderedClass
                  .getMethod("rendered")
                  .invoke(renderedClass.getDeclaredField(interp.global.nme.MODULE_INSTANCE_FIELD.toString).get())
                  .asInstanceOf[Widget]
                  .toHtml
            } else {
              // a line like println(...) is technically a val, but returns null for some reason
              // so wrap it in an option in case that happens...
              Option(line.call("$result")) map { result => Text(result.toString) } getOrElse NodeSeq.Empty
            }
          } else {
            NodeSeq.Empty
          }

          Success(evalValue)
        }
        catch {
          case NonFatal(e) => {
            val ex = new StringWriter()
            e.printStackTrace(new PrintWriter(ex))
            Failure(ex.toString)
          }
        }

      case ReplIncomplete => Incomplete
      case Error          => Failure(stdoutBytes.toString)
    }

    (result, stdoutBytes.toString)
  }

  def complete(line: String, cursorPosition: Int): (String, Seq[Match]) = {
    def literalCompletion(arg: String) = {
      val LiteralReg = """.*"([\w/]+)""".r
      arg match {
        case LiteralReg(literal) => Some(literal)
        case _ => None
      }
    }

    // CY: Don't ask to explain why this works.
    // Look at JLineCompletion.JLineTabCompletion.complete.mkDotted
    // The "regularCompletion" path is the only path that is (likely) to succeed
    // so we want access to that parsed version to pull out the part that was "matched"...
    // ...just...trust me.
    val delim = argCompletor.getDelimiter
    val list = delim.delimit(line, cursorPosition)
    val bufferPassedToCompletion = list.getCursorArgument
    val actCursorPosition = list.getArgumentPosition
    val parsed = Parsed.dotted(bufferPassedToCompletion, actCursorPosition) // withVerbosity verbosity
    val matchedText = bufferPassedToCompletion.takeRight(actCursorPosition - parsed.position)

    literalCompletion(bufferPassedToCompletion) match {
      case Some(literal) =>
        // strip any leading quotes
        stringCompletor.complete(literal)
      case None =>
        val candidates = getCompletions(line, cursorPosition)

        (matchedText, if (candidates.size > 0 && candidates.head.isEmpty) {
          List()
        } else {
          candidates.map(Match(_))
        })
    }
  }

  def objectInfo(line: String): Seq[String] = {
    // CY: The REPL is stateful -- it isn't until you ask to complete
    // the thing twice does it give you the method signature (i.e. you
    // hit tab twice).  So we simulate that here... (nutty, I know)
    getCompletions(line, line.length)
    val candidates = getCompletions(line, line.length)

    if (candidates.size >= 2 && candidates.head.isEmpty) {
      candidates.tail
    } else {
      Seq.empty
    }
  }

  /**
   * Reports the virtual directory populated by the Scala interpreter.
   *
   * @return the virtual directory populated by the Scala interpreter.
   */
  def getVirtualDirectory(): VirtualDirectory = {
    interp.virtualDirectory
  }
}

// -------------------------------------------------------------------------------------------------

object Repl extends Logging {
  private final val instances: mutable.Buffer[Repl] = mutable.Buffer()

  def listInstances(): Seq[Repl] = {
    return instances.toSeq
  }
}
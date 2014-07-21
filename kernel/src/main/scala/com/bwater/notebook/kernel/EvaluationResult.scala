/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */

package com.bwater.notebook.kernel

import scala.xml.NodeSeq

/**
 * Result of evaluating something in the REPL.
 *
 * Difference between Incomplete and Failure:
 *   <li> Incomplete: the expression failed to compile </li>
 *   <li> Failure: an exception was thrown while executing the code </li>
 */
sealed abstract class EvaluationResult

case object Incomplete extends EvaluationResult
case class Failure(stackTrace: String) extends EvaluationResult
case class Success(result: NodeSeq) extends EvaluationResult

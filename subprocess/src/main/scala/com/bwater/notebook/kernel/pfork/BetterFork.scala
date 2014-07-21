/*
 * Copyright (c) 2013  Bridgewater Associates, LP
 *
 * Distributed under the terms of the Modified BSD License.  The full license is in
 * the file COPYING, distributed as part of this software.
 */

package com.bwater.notebook.kernel.pfork

import java.io.EOFException
import java.io.File
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.net.URLClassLoader
import java.net.URLDecoder
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import scala.IndexedSeq
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.mutableMapAsJavaMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

import org.apache.commons.exec.CommandLine
import org.apache.commons.exec.DefaultExecutor
import org.apache.commons.exec.ExecuteException
import org.apache.commons.exec.ExecuteResultHandler
import org.apache.commons.exec.ExecuteWatchdog
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

trait ForkableProcess {
  /**
   * Called in the remote VM. Can return any useful information to the server through the return
   * @param args
   * @return
   */
  def init(args: Seq[String]): String
  def waitForExit()
}

// —————————————————————————————————————————————————————————————————————————————————————————————————

class BetterFork[A <: ForkableProcess : Manifest](executionContext: ExecutionContext) {
  import BetterFork._
  private implicit val ec = executionContext

  val processClass = manifest[A].erasure

  def workingDirectory = new File(".")
  def heap: Long = defaultHeap
  def stack: Long = -1
  def permGen: Long = -1
  def reservedCodeCache: Long = -1
  def server: Boolean = true
  def debug: Boolean = false // If true, then you will likely get address in use errors spawning multiple processes
  def classPath: IndexedSeq[String] = defaultClassPath
  def classPathString = classPath.mkString(File.pathSeparator)

//  def bootClassPath: IndexedSeq[String] = sys.props("sun.boot.class.path").split(File.pathSeparator)
//  def bootClassPathString = bootClassPath.mkString(File.pathSeparator)

//  val kijiClassPath = "/R/kiji-bento/current/bin/../lib/kiji-schema-1.5.0.jar::/R/kiji-bento/current/bin/../conf::/R/kiji-bento/current/bin/../lib/*:/R/kiji-bento/current/bin/../lib/distribution/hadoop2/*:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/conf:/R/jdk/7.60.17/lib/tools.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/hadoop-core-2.0.0-mr1-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/activation-1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/ant-contrib-1.0b3.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/asm-3.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/avro-1.7.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/avro-compiler-1.7.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-beanutils-1.7.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-beanutils-core-1.8.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-cli-1.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-codec-1.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-collections-3.2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-compress-1.4.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-configuration-1.6.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-digester-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-el-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-httpclient-3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-io-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-lang-2.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-logging-1.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-math-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-net-3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/guava-11.0.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-annotations-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-auth-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-common-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-common-2.0.0-cdh4.3.0-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-fairscheduler-2.0.0-mr1-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-hdfs-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hsqldb-1.8.0.10.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-core-asl-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-jaxrs-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-mapper-asl-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-xc-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jasper-compiler-5.5.23.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jasper-runtime-5.5.23.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jaxb-api-2.2.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jaxb-impl-2.2.3-1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jersey-core-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jersey-json-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jersey-server-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jets3t-0.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jettison-1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jetty-6.1.26.cloudera.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jetty-util-6.1.26.cloudera.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jline-0.9.94.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsch-0.1.42.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsp-api-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsr305-1.3.9.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/junit-4.8.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/kfs-0.2.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/kfs-0.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/log4j-1.2.17.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/mockito-all-1.8.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/paranamer-2.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/protobuf-java-2.4.0a.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/servlet-api-2.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/slf4j-api-1.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/slf4j-log4j12-1.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/snappy-java-1.0.4.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/stax-api-1.0.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/xmlenc-0.52.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/xz-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/zookeeper-3.4.5-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsp-2.1/jsp-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsp-2.1/jsp-api-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/conf:/R/jdk/7.60.17/lib/tools.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/hbase-0.94.6-cdh4.3.0-security.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/hbase-0.94.6-cdh4.3.0-security-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/activation-1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/aopalliance-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/asm-3.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/avro-1.7.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-beanutils-1.7.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-beanutils-core-1.8.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-cli-1.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-codec-1.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-collections-3.2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-compress-1.4.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-configuration-1.6.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-daemon-1.0.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-digester-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-el-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-httpclient-3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-io-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-lang-2.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-logging-1.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/commons-net-3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/core-3.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/gmbal-api-only-3.0.0-b023.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/grizzly-framework-2.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/grizzly-framework-2.1.1-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/grizzly-http-2.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/grizzly-http-server-2.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/grizzly-http-servlet-2.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/grizzly-rcm-2.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/guava-11.0.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/guice-3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/guice-servlet-3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-annotations-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-auth-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-client-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-common-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-common-2.0.0-cdh4.3.0-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-hdfs-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-hdfs-2.0.0-cdh4.3.0-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-app-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-common-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-core-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-hs-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-jobclient-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-jobclient-2.0.0-cdh4.3.0-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-mapreduce-client-shuffle-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-minicluster-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-api-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-client-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-common-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-server-common-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-server-nodemanager-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-server-resourcemanager-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-server-tests-2.0.0-cdh4.3.0-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/hadoop-yarn-server-web-proxy-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/high-scale-lib-1.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/httpclient-4.1.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/httpcore-4.1.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jackson-core-asl-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jackson-jaxrs-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jackson-mapper-asl-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jackson-xc-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jamon-runtime-2.3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jasper-compiler-5.5.23.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jasper-runtime-5.5.23.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/javax.inject-1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/javax.servlet-3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jaxb-api-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jaxb-impl-2.2.3-1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-client-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-core-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-grizzly2-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-guice-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-json-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-server-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-test-framework-core-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jersey-test-framework-grizzly2-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jets3t-0.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jettison-1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jetty-6.1.26.cloudera.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jetty-util-6.1.26.cloudera.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jruby-complete-1.6.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jsch-0.1.42.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jsp-2.1-6.1.14.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jsp-api-2.1-6.1.14.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jsp-api-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/jsr305-1.3.9.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/junit-4.10-HBASE-1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/kfs-0.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/libthrift-0.9.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/log4j-1.2.17.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/management-api-3.0.0-b012.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/metrics-core-2.1.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/netty-3.2.4.Final.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/paranamer-2.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/protobuf-java-2.4.0a.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/servlet-api-2.5-6.1.14.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/servlet-api-2.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/slf4j-api-1.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/slf4j-log4j12-1.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/snappy-java-1.0.4.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/stax-api-1.0.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/xmlenc-0.52.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/xz-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hbase-0.94.6-cdh4.3.0/lib/zookeeper-3.4.5-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/conf:/R/jdk/7.60.17/lib/tools.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/hadoop-core-2.0.0-mr1-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/activation-1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/ant-contrib-1.0b3.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/asm-3.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/avro-1.7.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/avro-compiler-1.7.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-beanutils-1.7.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-beanutils-core-1.8.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-cli-1.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-codec-1.4.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-collections-3.2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-compress-1.4.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-configuration-1.6.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-digester-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-el-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-httpclient-3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-io-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-lang-2.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-logging-1.1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-math-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/commons-net-3.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/guava-11.0.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-annotations-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-auth-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-common-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-common-2.0.0-cdh4.3.0-tests.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-fairscheduler-2.0.0-mr1-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hadoop-hdfs-2.0.0-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/hsqldb-1.8.0.10.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-core-asl-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-jaxrs-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-mapper-asl-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jackson-xc-1.8.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jasper-compiler-5.5.23.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jasper-runtime-5.5.23.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jaxb-api-2.2.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jaxb-impl-2.2.3-1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jersey-core-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jersey-json-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jersey-server-1.8.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jets3t-0.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jettison-1.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jetty-6.1.26.cloudera.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jetty-util-6.1.26.cloudera.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jline-0.9.94.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsch-0.1.42.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsp-api-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsr305-1.3.9.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/junit-4.8.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/kfs-0.2.2.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/kfs-0.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/log4j-1.2.17.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/mockito-all-1.8.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/paranamer-2.3.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/protobuf-java-2.4.0a.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/servlet-api-2.5.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/slf4j-api-1.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/slf4j-log4j12-1.6.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/snappy-java-1.0.4.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/stax-api-1.0.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/xmlenc-0.52.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/xz-1.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/zookeeper-3.4.5-cdh4.3.0.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsp-2.1/jsp-2.1.jar:/R/kiji-bento/2.1.0/cluster/lib/hadoop-2.0.0-mr1-cdh4.3.0/lib/jsp-2.1/jsp-api-2.1.jar"

  def jvmArgs = {
    val builder = IndexedSeq.newBuilder[String]

    def ifNonNeg(value: Long, prefix: String) {
      if (value >= 0) {
        builder += (prefix + value)
      }
    }

    // builder ++= Seq("-Xbootclasspath/a:" + bootClassPathString + ":" + "/R/scala/current/lib/jline.jar")
    ifNonNeg(heap, "-Xmx")
    ifNonNeg(stack, "-Xss")
    ifNonNeg(permGen, "-XX:MaxPermSize=")
    ifNonNeg(reservedCodeCache, "-XX:ReservedCodeCacheSize=")
    if (server) builder += "-server"
    if (debug) builder ++= IndexedSeq("-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005")
    builder.result()
  }

  def execute(
      args: String*
  ): Future[ProcessInfo] = {
    /* DK: Bi-directional liveness can be detected via redirected System.in (child), System.out (parent), avoids need for socket... */
    val ss = new ServerSocket(0)
    val cmd = new CommandLine(sys.props("java.home") + "/bin/java")
      .addArguments(jvmArgs.toArray)
      .addArgument(classOf[ChildProcessMain].getName)
      .addArgument(processClass.getName)
      .addArgument(ss.getLocalPort.toString)
      .addArguments(args.toArray)

    Future {
      log.info("Spawning Java subprocess:\n%s\nwith classpath:\n%s".format(
          (Seq(cmd.getExecutable) ++ cmd.getArguments).map { arg => "'%s'".format(arg) }.mkString(" \\\n\t"),
          classPath.map { cp => "\t" + cp }.mkString("\n")
      ))

      // use environment because classpaths can be longer here than as a command line arg
      val environment = System.getenv + ("CLASSPATH" -> classPathString)
      val exec = new KillableExecutor

      val completion = Promise[Int]
      exec.setWorkingDirectory(workingDirectory)
      exec.execute(cmd, environment, new ExecuteResultHandler {
        def onProcessFailed(e: ExecuteException) {}
        def onProcessComplete(exitValue: Int) { completion.success(exitValue)}
      })
      val socket = ss.accept()
      serverSockets += socket
      try {
        val ois = new ObjectInputStream(socket.getInputStream)
        val resp = ois.readObject().asInstanceOf[String]
        new ProcessInfo(() => exec.kill(), resp, completion.future)
      } catch {
        case ex:SocketException => throw new ExecuteException("Failed to start process %s".format(cmd), 1, ex)
        case ex:EOFException =>    throw new ExecuteException("Failed to start process %s".format(cmd), 1, ex)
      }
    }
  }
}

class ProcessInfo(killer: () => Unit, val initReturn: String, val completion: Future[Int]) {
  def kill() { killer() }
}

// —————————————————————————————————————————————————————————————————————————————————————————————————

object BetterFork {
  private final val log = LoggerFactory.getLogger(classOf[BetterFork[_]])

  // Keeps server sockets around so they are not GC'd
  private final val serverSockets = new ListBuffer[Socket]()

  def defaultClassPath: IndexedSeq[String] = {
    val loader = getClass.getClassLoader.asInstanceOf[URLClassLoader]

    loader.getURLs map {
      u =>
        val f = new File(u.getFile)
        URLDecoder.decode(f.getAbsolutePath, "UTF8")
    }
  }

  def defaultHeap = Runtime.getRuntime.maxMemory

  /* Override to expose ability to forcibly kill the process */
  private class KillableExecutor extends DefaultExecutor {
    val killed = new AtomicBoolean(false)
    setWatchdog(new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT) {
      override def start(p: Process) { if (killed.get()) p.destroy() }
    })
    def kill() {
      if (killed.compareAndSet(false, true))
        Option(getExecutorThread()) foreach(_.interrupt())
    }
  }

  /** Sub-process entry point (through ChildProcessMain) */
  private[pfork] def main(args: Array[String]) {
    val className = args(0)
    val parentPort = args(1).toInt

    PropertyConfigurator.configure(getClass.getResource("/log4j.subprocess.properties"))
    log.info("Remote process starting for {}", className)

    val socket = new Socket("127.0.0.1", parentPort)
    val remainingArgs = args.drop(2).toIndexedSeq
    val hostedClass = Class.forName(className).newInstance().asInstanceOf[ForkableProcess]
    val result = hostedClass.init(remainingArgs)
    val oos = new ObjectOutputStream(socket.getOutputStream)
    oos.writeObject(result)
    oos.flush()

    val executorService = Executors.newFixedThreadPool(10)
    implicit val ec = ExecutionContext.fromExecutorService(executorService)

    val parentDone = Future { socket.getInputStream.read() }
    val localDone = Future { hostedClass.waitForExit() }

    val done = Future.firstCompletedOf(Seq(parentDone, localDone))

    try {
      Await.result(done, Duration.Inf)
    } finally {
      log.warn("Parent process %s or forked process %s stopped: exiting."
          .format("localhost:%s".format(parentPort), className))
      sys.exit(0)
    }
  }
}

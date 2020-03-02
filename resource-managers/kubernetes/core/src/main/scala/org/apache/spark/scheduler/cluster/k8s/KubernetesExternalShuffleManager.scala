/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.scheduler.cluster.k8s

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.internal.readiness.Readiness
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.KubernetesUtils
import org.apache.spark.internal.Logging
import org.apache.spark.network.shuffle.kubernetes.KubernetesExternalShuffleClient

private[spark] trait KubernetesExternalShuffleManager {
  def start(appId: String): Unit

  def stop(): Unit

  /**
   * Returns the properties that should be applied for this executor pod, given that
   * this executor will need to communicate with an external shuffle service.
   *
   * In practice, this seq will always have a size of 1, but since this method's semantics are that
   * the returned values are key-value pairs to apply as properties, it is clearer to express
   * this as a collection.
   */
  def getShuffleServiceConfigurationForExecutor(executorPod: Pod): Seq[(String, String)]
}

private[spark] class KubernetesExternalShuffleManagerImpl(
                                                           sparkConf: SparkConf,
                                                           client: KubernetesClient,
                                                           shuffleClient: KubernetesExternalShuffleClient)
  extends KubernetesExternalShuffleManager with Logging {

  private val shuffleNamespace = sparkConf.get(KUBERNETES_SHUFFLE_NAMESPACE)
  private val shufflePodLabels = KubernetesUtils.parseKeyValuePairs(
    sparkConf.get(KUBERNETES_SHUFFLE_LABELS),
    KUBERNETES_SHUFFLE_LABELS.key,
    "shuffle-labels")
  if (shufflePodLabels.isEmpty) {
    throw new SparkException(s"Dynamic allocation enabled " +
      s"but no ${KUBERNETES_SHUFFLE_LABELS.key} specified")
  }
  private val externalShufflePort = sparkConf.getInt("spark.shuffle.service.port", 7337)
  private val shufflePodCache = scala.collection.mutable.Map[String, String]()
  private var watcher: Watch = _

  override def start(appId: String): Unit = {
    // seed the initial cache.
    val pods = client.pods()
      .inNamespace(shuffleNamespace)
      .withLabels(shufflePodLabels.asJava)
      .list()
    pods.getItems.asScala.foreach {
      pod =>
        if (Readiness.isReady(pod)) {
          addShufflePodToCache(pod)
        } else {
          logWarning(s"Found unready shuffle pod ${pod.getMetadata.getName} " +
            s"on node ${pod.getSpec.getNodeName}")
        }
    }

    watcher = client
      .pods()
      .inNamespace(shuffleNamespace)
      .withLabels(shufflePodLabels.asJava)
      .watch(new Watcher[Pod] {
        override def eventReceived(action: Watcher.Action, p: Pod): Unit = {
          action match {
            case Action.DELETED | Action.ERROR =>
              shufflePodCache.remove(p.getSpec.getNodeName)
            case Action.ADDED | Action.MODIFIED if Readiness.isReady(p) =>
              addShufflePodToCache(p)
          }
        }
        override def onClose(e: KubernetesClientException): Unit = {}
      })
    shuffleClient.init(appId)
  }

  private def addShufflePodToCache(pod: Pod): Unit = shufflePodCache.synchronized {
    if (shufflePodCache.contains(pod.getSpec.getNodeName)) {
      val registeredPodName = shufflePodCache(pod.getSpec.getNodeName)
      if (registeredPodName.equals(pod.getStatus.getPodIP)) {
        logWarning(s"The same pod $registeredPodName is added again on ${pod.getSpec.getNodeName}")
      } else {
        throw new SparkException(s"Ambiguous specification of shuffle service pod. " +
          s"Found multiple matching pods: ${pod.getMetadata.getName}, " +
          s"$registeredPodName on ${pod.getSpec.getNodeName}")
      }
    } else {
      shufflePodCache(pod.getSpec.getNodeName) = pod.getStatus.getPodIP
    }
  }

  override def stop(): Unit = {
    watcher.close()
    shuffleClient.close()
  }

  override def getShuffleServiceConfigurationForExecutor(executorPod: Pod)
  : Seq[(String, String)] = {
    val nodeName = executorPod.getSpec.getNodeName
    val shufflePodIp = shufflePodCache.synchronized {
      shufflePodCache.getOrElse(nodeName,
        throw new SparkException(s"Unable to find shuffle pod on node $nodeName"))
    }
    // Inform the shuffle pod about this application so it can watch.
    shuffleClient.registerDriverWithShuffleService(
      shufflePodIp,
      externalShufflePort,
      sparkConf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs",
        s"${sparkConf.getTimeAsSeconds("spark.network.timeout", "120s")}s"),
      sparkConf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")
    )
    Seq((SPARK_SHUFFLE_SERVICE_HOST.key, shufflePodIp))
  }
}
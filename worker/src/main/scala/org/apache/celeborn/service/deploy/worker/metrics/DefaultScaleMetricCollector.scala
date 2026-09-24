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

package org.apache.celeborn.service.deploy.worker.metrics

import java.lang.management.ManagementFactory
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics._
import org.apache.celeborn.common.util.{ThreadUtils, Utils}
import org.apache.celeborn.service.deploy.worker.Worker

class DefaultScaleMetricCollector(conf: CelebornConf)
  extends ScaleMetricCollector with Logging {

  private val memoryWindow = new MetricSlidingWindow(conf.scaleMetricWindowSize)
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("scale-metric-collector")
  private var scheduledTask: ScheduledFuture[_] = _
  private var worker: Worker = _

  @volatile private var currentStats: Option[WorkerStats] = None

  override def init(worker: Worker): Unit = {
    this.worker = worker
    scheduledTask = executor.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = Utils.tryLogNonFatalError(collectOnce(worker))
      },
      0,
      conf.scaleMetricInterval,
      TimeUnit.MILLISECONDS)
  }

  override def stop(): Unit = {
    if (scheduledTask != null) {
      scheduledTask.cancel(true)
      scheduledTask = null
    }
    executor.shutdownNow()
  }

  override def currentWorkerStats(): Option[WorkerStats] = currentStats

  private[metrics] def collectOnce(worker: Worker): Unit = {
    val now = System.currentTimeMillis()
    val metrics = Seq(
      memoryMetric(worker, now),
      systemLoadMetric(now)) ++ diskMetrics(worker, now)
    currentStats = Some(WorkerStats(now, metrics))
  }

  private def memoryMetric(worker: Worker, now: Long): WorkerMetric = {
    val maxDirectMemory = worker.memoryManager.maxDirectMemory
    if (maxDirectMemory <= 0) {
      return unavailable(
        WorkerStats.NettyMemoryUsedRatio,
        MetricUnit.Ratio,
        now,
        memoryWindow.size,
        memoryWindow.startTime,
        memoryWindow.endTime)
    }

    memoryWindow.update(worker.memoryManager.getMemoryUsage, now)
    memoryWindow.average match {
      case Some(average) =>
        WorkerMetric(
          WorkerStats.NettyMemoryUsedRatio,
          Some(DoubleMetricValue(average / maxDirectMemory)),
          MetricUnit.Ratio,
          MetricStatus.Valid,
          now,
          memoryWindow.size,
          memoryWindow.startTime,
          memoryWindow.endTime)
      case None =>
        WorkerMetric(
          WorkerStats.NettyMemoryUsedRatio,
          None,
          MetricUnit.Ratio,
          MetricStatus.WarmingUp,
          now,
          memoryWindow.size,
          memoryWindow.startTime,
          memoryWindow.endTime)
    }
  }

  private def systemLoadMetric(now: Long): WorkerMetric = {
    val operatingSystem = ManagementFactory.getOperatingSystemMXBean
    val load = operatingSystem.getSystemLoadAverage
    val processors = operatingSystem.getAvailableProcessors
    if (load < 0 || processors <= 0) {
      unavailable(
        WorkerStats.LastMinuteSystemLoadRatio,
        MetricUnit.Ratio,
        now,
        1,
        now,
        now)
    } else {
      WorkerMetric(
        WorkerStats.LastMinuteSystemLoadRatio,
        Some(DoubleMetricValue(load / processors)),
        MetricUnit.Ratio,
        MetricStatus.Valid,
        now,
        1,
        now,
        now)
    }
  }

  private def diskMetrics(worker: Worker, now: Long): Seq[WorkerMetric] = {
    val disks = worker.storageManager.allDisksSnapshot()
    val usableBytes = disks.map(_.actualUsableSpace).sum
    val totalBytes = disks.map(_.totalSpace).sum
    if (disks.isEmpty || totalBytes <= 0 || usableBytes < 0 || usableBytes > totalBytes) {
      Seq(
        unavailable(WorkerStats.DiskUsedRatio, MetricUnit.Ratio, now, disks.size, now, now),
        unavailable(WorkerStats.DiskRemainingSize, MetricUnit.Bytes, now, disks.size, now, now))
    } else {
      Seq(
        WorkerMetric(
          WorkerStats.DiskUsedRatio,
          Some(DoubleMetricValue(1.0 - usableBytes / totalBytes.toDouble)),
          MetricUnit.Ratio,
          MetricStatus.Valid,
          now,
          disks.size,
          now,
          now),
        WorkerMetric(
          WorkerStats.DiskRemainingSize,
          Some(LongMetricValue(usableBytes)),
          MetricUnit.Bytes,
          MetricStatus.Valid,
          now,
          disks.size,
          now,
          now))
    }
  }

  private def unavailable(
      name: String,
      unit: MetricUnit,
      now: Long,
      sampleCount: Int,
      windowStartTime: Long,
      windowEndTime: Long): WorkerMetric = {
    WorkerMetric(
      name,
      None,
      unit,
      MetricStatus.Unavailable,
      now,
      sampleCount,
      windowStartTime,
      windowEndTime)
  }
}

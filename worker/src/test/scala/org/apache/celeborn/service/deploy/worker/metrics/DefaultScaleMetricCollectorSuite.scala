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

import org.mockito.MockitoSugar._
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.meta.DiskInfo
import org.apache.celeborn.common.metrics._
import org.apache.celeborn.service.deploy.worker.Worker
import org.apache.celeborn.service.deploy.worker.memory.MemoryManager
import org.apache.celeborn.service.deploy.worker.storage.StorageManager

class DefaultScaleMetricCollectorSuite extends AnyFunSuite {
  test("collects valid memory, load, and disk metrics") {
    val conf = new CelebornConf()
      .set("celeborn.scale.metricWindowSize", "1")
    val worker = mock[Worker]
    val memoryManager = mock[MemoryManager]
    val storageManager = mock[StorageManager]
    val disk = new DiskInfo("/tmp", 512L, 0L, 0L, 0L)
    disk.setTotalSpace(1024L)

    when(worker.memoryManager).thenReturn(memoryManager)
    when(worker.storageManager).thenReturn(storageManager)
    memoryManager.maxDirectMemory = 1024L
    when(memoryManager.getMemoryUsage).thenReturn(256L)
    when(storageManager.allDisksSnapshot()).thenReturn(List(disk))

    val collector = new DefaultScaleMetricCollector(conf)
    collector.collectOnce(worker)
    val stats = collector.currentWorkerStats()

    assert(stats.isDefined)
    val metrics = stats.get.metrics.map(metric => metric.name -> metric).toMap
    assert(metrics(WorkerStats.NettyMemoryUsedRatio).value === Some(DoubleMetricValue(0.25)))
    assert(metrics(WorkerStats.LastMinuteSystemLoadRatio).status === MetricStatus.Valid)
    assert(metrics(WorkerStats.DiskUsedRatio).value === Some(DoubleMetricValue(0.5)))
    assert(metrics(WorkerStats.DiskRemainingSize).value === Some(LongMetricValue(512L)))
  }
}

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

package org.apache.celeborn.common.metrics

import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.util.PbSerDeUtils

class WorkerStatsSuite extends AnyFunSuite {
  test("round trips worker stats") {
    val stats = WorkerStats(
      timestamp = 100L,
      metrics = Seq(
        WorkerMetric(
          WorkerStats.NettyMemoryUsedRatio,
          Some(DoubleMetricValue(0.5)),
          MetricUnit.Ratio,
          MetricStatus.Valid,
          observationTime = 99L,
          sampleCount = 6,
          windowStartTime = 50L,
          windowEndTime = 99L),
        WorkerMetric(
          WorkerStats.DiskRemainingSize,
          Some(LongMetricValue(1024L)),
          MetricUnit.Bytes,
          MetricStatus.Valid,
          observationTime = 99L,
          sampleCount = 1,
          windowStartTime = 99L,
          windowEndTime = 99L)))

    val decoded = PbSerDeUtils.fromPbWorkerStats(PbSerDeUtils.toPbWorkerStats(stats))

    assert(decoded === stats)
  }

  test("keeps zero and unavailable metrics distinct") {
    val zero = WorkerMetric(
      WorkerStats.NettyMemoryUsedRatio,
      Some(DoubleMetricValue(0.0)),
      MetricUnit.Ratio,
      MetricStatus.Valid,
      observationTime = 99L,
      sampleCount = 6,
      windowStartTime = 50L,
      windowEndTime = 99L)
    val unavailable = zero.copy(value = None, status = MetricStatus.Unavailable)

    assert(zero.value.isDefined)
    assert(zero.status === MetricStatus.Valid)
    assert(unavailable.value.isEmpty)
    assert(unavailable.status === MetricStatus.Unavailable)
  }
}

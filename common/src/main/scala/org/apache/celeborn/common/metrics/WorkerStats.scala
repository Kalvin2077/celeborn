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

/** A single worker metric and the metadata needed to interpret it. */
case class WorkerMetric(
    name: String,
    value: Option[MetricValue],
    unit: MetricUnit,
    status: MetricStatus,
    observationTime: Long,
    sampleCount: Int,
    windowStartTime: Long,
    windowEndTime: Long)

/** The latest metrics collected from one worker. */
case class WorkerStats(
    timestamp: Long,
    metrics: Seq[WorkerMetric])

sealed trait MetricValue

case class DoubleMetricValue(value: Double) extends MetricValue

case class LongMetricValue(value: Long) extends MetricValue

sealed trait MetricUnit

object MetricUnit {
  case object Ratio extends MetricUnit
  case object Bytes extends MetricUnit
  case object BytesPerSecond extends MetricUnit
}

sealed trait MetricStatus

object MetricStatus {
  case object Valid extends MetricStatus
  case object WarmingUp extends MetricStatus
  case object Unavailable extends MetricStatus
  case object Failed extends MetricStatus
}

object WorkerStats {
  val NettyMemoryUsedRatio = "NettyMemoryUsedRatio"
  val LastMinuteSystemLoadRatio = "LastMinuteSystemLoadRatio"
  val DiskUsedRatio = "DiskUsedRatio"
  val DiskRemainingSize = "DiskRemainingSize"
}

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

class MetricSlidingWindow(windowSize: Int) {
  require(windowSize > 0, "Metric window size must be positive")

  private val samples = new Array[Long](windowSize)
  private val timestamps = new Array[Long](windowSize)
  private var count = 0
  private var head = 0
  private var sum = 0L

  def update(sample: Long, timestamp: Long): Unit = {
    if (count < windowSize) {
      count += 1
    } else {
      sum -= samples(head)
    }
    samples(head) = sample
    timestamps(head) = timestamp
    sum += sample
    head = (head + 1) % windowSize
  }

  def average: Option[Double] = {
    if (count < windowSize) {
      None
    } else {
      Some(sum / windowSize.toDouble)
    }
  }

  def size: Int = count

  def startTime: Long = {
    if (count == 0) {
      0L
    } else if (count < windowSize) {
      timestamps(0)
    } else {
      timestamps(head)
    }
  }

  def endTime: Long = {
    if (count == 0) {
      0L
    } else {
      timestamps((head - 1 + windowSize) % windowSize)
    }
  }
}

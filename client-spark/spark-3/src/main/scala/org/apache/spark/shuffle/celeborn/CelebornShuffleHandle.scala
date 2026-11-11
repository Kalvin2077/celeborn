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

package org.apache.spark.shuffle.celeborn

import org.apache.spark.ShuffleDependency
import org.apache.spark.shuffle.BaseShuffleHandle

import org.apache.celeborn.common.identity.UserIdentifier

// k Driver 在 registerShuffle 中创建 handle，Executor 侧的 getWriter/getReader 都会消费它。
// k BaseShuffleHandle 已保存 Spark 逻辑 shuffleId 和 ShuffleDependency。
// k CelebornShuffleHandle 再补充定位 LifecycleManager/ShuffleClient 所需的信息；它不包含 shuffle 数据。
class CelebornShuffleHandle[K, V, C](
    // k Celeborn application 的唯一标识，也是复用 ShuffleClient 的维度。
    val appUniqueId: String,
    // k Driver 内 LifecycleManager 的 RPC host，不是 Master/Worker 地址。
    val lifecycleManagerHost: String,
    // k 与 host 共同组成 Executor 回连 LifecycleManager 的控制面地址。
    val lifecycleManagerPort: Int,
    // k Celeborn 的用户/资源归属标识，创建 ShuffleClient 时一并传入。
    val userIdentifier: UserIdentifier,
    // k Spark 分配的逻辑 shuffleId；stage rerun 开启后运行时再映射到 Celeborn shuffleId。
    shuffleId: Int,
    // k 是否允许 stage 重跑；开启后一个 Spark shuffleId 可对应多个 Celeborn shuffleId。
    val stageRerunEnabled: Boolean,
    // k 上游 RDD 的 partition 数，也就是这次 shuffle 期待完成的 mapId 数。
    val numMappers: Int,
    // k Spark 的 shuffle 语义：partitioner、serializer、aggregator、ordering 等。
    dependency: ShuffleDependency[K, V, C],
    // k 交给派生 ShuffleClient 的透传字节；默认构造器传 null。
    val extension: Array[Byte])
  extends BaseShuffleHandle(shuffleId, dependency) {
  def this(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      userIdentifier: UserIdentifier,
      shuffleId: Int,
      stageRerunEnabled: Boolean,
      numMappers: Int,
      dependency: ShuffleDependency[K, V, C]) = this(
    appUniqueId,
    lifecycleManagerHost,
    lifecycleManagerPort,
    userIdentifier,
    shuffleId,
    stageRerunEnabled,
    numMappers,
    dependency,
    null)
}

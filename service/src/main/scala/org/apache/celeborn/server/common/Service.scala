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

package org.apache.celeborn.server.common

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.metrics.MetricsSystem
import org.apache.celeborn.server.common.service.config.{ConfigService, DynamicConfigServiceFactory}

// k Celeborn 服务端组件生命周期基类，是对服务的基本抽象。
// k 通过 extends 拓展日志能力
abstract class Service extends Logging {
  // k 名字
  def serviceName: String

  // k 配置
  def conf: CelebornConf

  // k 观测
  def metricsSystem: MetricsSystem

  // k 动态配置
  def configService: ConfigService = DynamicConfigServiceFactory.getConfigService(conf)

  // k 通用的生命周期定义
  def initialize(): Unit = {
    // k 基类初始化负责启动观测
    if (conf.metricsSystemEnable) {
      logInfo(s"Metrics system enabled.")
      metricsSystem.start()
    }
  }

  // k stop 留给子类扩展
  def stop(exitKind: Int): Unit = {}
}

// k 伴生对象（静态），存放静态变量
object Service {
  val MASTER = "master"
  val WORKER = "worker"
}

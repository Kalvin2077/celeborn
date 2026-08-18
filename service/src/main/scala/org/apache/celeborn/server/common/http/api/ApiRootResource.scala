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

package org.apache.celeborn.server.common.http.api

import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

import org.apache.celeborn.server.common.HttpService

// k 把 Jersey REST API 封装成 Jetty 可挂载的 Handler。
// k 也把 http service 放入 context 供 resource 调用。
private[celeborn] object ApiRootResource {
  def getServletHandler(rs: HttpService): ServletContextHandler = {
    val openapiConf: ResourceConfig = new OpenAPIConfig(rs.serviceName)
    // k 创建 Jersey Servlet 用于处理 REST API
    val holder = new ServletHolder(new ServletContainer(openapiConf))
    // k 将 Servelet 放入 jetty Server，计划长期运行
    // ? 为什么不使用 HTTP Session
    // k http 默认无状态，sessions 用于增强状态能力，celeborn 不需要登陆会话
    val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    handler.setContextPath("/")
    // k 将 service 与 servlet 绑定
    HttpServiceContext.set(handler, rs)
    // k jetty Server 将所有 /* 请求交给 hodler 也就是 Jersey Servlet
    handler.addServlet(holder, "/*")
    handler
  }
}

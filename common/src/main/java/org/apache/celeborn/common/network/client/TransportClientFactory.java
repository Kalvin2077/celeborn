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

package org.apache.celeborn.common.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.client.MasterNotLeaderException;
import org.apache.celeborn.common.exception.CelebornIOException;
import org.apache.celeborn.common.network.TransportContext;
import org.apache.celeborn.common.network.sasl.registration.RegistrationClientBootstrap;
import org.apache.celeborn.common.network.server.TransportChannelHandler;
import org.apache.celeborn.common.network.util.*;
import org.apache.celeborn.common.util.ExceptionUtils;
import org.apache.celeborn.common.util.JavaUtils;
import org.apache.celeborn.common.util.Utils;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 *
 * <p>The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for all
 * TransportClients.
 *
 * <p>TransportClients will be reused whenever possible.
 */
public class TransportClientFactory implements Closeable {

  /** A simple data structure to track the pool of clients between two peer nodes. */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final List<TransportClientBootstrap> clientBootstraps;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** Random number generator for picking connections between peers. */
  private final Random rand;

  private final int numConnectionsPerPeer;

  private final int connectTimeoutMs;
  private final int connectionTimeoutMs;
  private final int sslHandshakeTimeoutMs;

  private final int receiveBuf;

  private final int sendBuf;
  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  protected ByteBufAllocator allocator;
  private final int maxClientConnectRetries;
  private final int maxClientConnectRetryWaitTimeMs;

  public TransportClientFactory(
      TransportContext context, List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    TransportConf conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    this.connectionPool = JavaUtils.newConcurrentHashMap();
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.connectTimeoutMs = conf.connectTimeoutMs();
    this.connectionTimeoutMs = conf.connectionTimeoutMs();
    this.sslHandshakeTimeoutMs = conf.sslHandshakeTimeoutMs();
    this.receiveBuf = conf.receiveBuf();
    this.sendBuf = conf.sendBuf();
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    logger.info("Module {} mode {} threads {}", conf.getModuleName(), ioMode, conf.clientThreads());
    this.workerGroup =
        NettyUtils.createEventLoop(
            ioMode,
            conf.clientThreads(),
            conf.conflictAvoidChooserEnable(),
            conf.getModuleName() + "-client");
    // Always disable thread-local cache when creating pooled ByteBuf allocator for TransportClients
    // because the ByteBufs are allocated by the event loop thread, but released by the executor
    // thread rather than the event loop thread. Those thread-local caches actually delay the
    // recycling of buffers, leading to larger memory usage.
    this.allocator =
        NettyUtils.getByteBufAllocator(conf, context.getSource(), false, conf.clientThreads());
    this.maxClientConnectRetries = conf.maxIORetries();
    this.maxClientConnectRetryWaitTimeMs = conf.ioRetryWaitTimeMs();
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * <p>We maintains an array of clients (size determined by
   * celeborn.$module.io.numConnectionsPerPeer) and randomly picks one to use. If no client was
   * previously created in the randomly selected spot, this function creates a new client and places
   * it there.
   *
   * <p>This blocks until a connection is successfully established and fully bootstrapped.
   *
   * <p>Concurrency: This method is safe to call from multiple threads.
   */
  public TransportClient createClient(String remoteHost, int remotePort, int partitionId)
      throws IOException, InterruptedException {
    return retryCreateClient(remoteHost, remotePort, partitionId, TransportFrameDecoder::new);
  }

  public TransportClient retryCreateClient(
      String remoteHost,
      int remotePort,
      int partitionId,
      Supplier<ChannelInboundHandlerAdapter> supplier)
      throws IOException, InterruptedException {
    int numTries = 0;
    while (numTries < maxClientConnectRetries) {
      try {
        return createClient(remoteHost, remotePort, partitionId, supplier.get());
      } catch (Exception e) {
        InterruptedException interruptedException = ExceptionUtils.findInterruptedException(e);
        if (interruptedException != null) {
          Thread.currentThread().interrupt();
          throw interruptedException;
        }
        numTries++;
        logger.warn(
            "Retry create client, times {}/{} with error: {}",
            numTries,
            maxClientConnectRetries,
            e.getMessage(),
            e);
        if (numTries == maxClientConnectRetries) {
          throw e;
        }

        Thread.sleep(maxClientConnectRetryWaitTimeMs);
      }
    }

    return null;
  }

  public TransportClient createClient(
      String remoteHost, int remotePort, int partitionId, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we create a client.
    // ? 为什么用 hostname 而不是 DNS 解析后的值?
    // k 使用 unresolved address 来作为缓存 cached client 的 key，足矣，如果用 ip，则每次都要进行 DNS 解析。
    final InetSocketAddress unresolvedAddress =
        InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    // k ClientPool 包含多个 Client，每个 Client 都有 lock
    // ? 天平：numConnectionsPerPeer
    // k 生产使用 numConnectionsPerPeer = 2，这里存在天平，网络吞吐 vs cpu/mem 开销，2 是经验值。
    ClientPool clientPool =
        connectionPool.computeIfAbsent(
            unresolvedAddress, key -> new ClientPool(numConnectionsPerPeer));
    // k 按照 paritionId 选择使用哪个 Client，利用 partitionId 是连续数列来均匀分配
    int clientIndex =
        partitionId < 0 ? rand.nextInt(numConnectionsPerPeer) : partitionId % numConnectionsPerPeer;
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      // k 刷新 cachedClient 的最后使用时间，避免被服务端关闭连接
      // k 取出底层 Netty Channel，pipeline 的处理器链（集合），除了 Decoder、Encoder，Handler 也是其中之一。
      // k handler 是消息处理器。
      TransportChannelHandler handler =
          cachedClient.getChannel().pipeline().get(TransportChannelHandler.class);
      if (handler != null) {
        synchronized (handler) {
          // k 刷新 cachedClient 的最后使用时间。避免前值覆盖后值。
          handler.getResponseHandler().updateTimeOfLastRequest();
        }
      }

      // ? 为什么要再检查一次
      // k A 走到这里，client 可能被 B 关闭
      if (cachedClient.isActive()) {
        logger.debug(
            "Returning cached connection from {} to {}: {}",
            cachedClient.getChannel().localAddress(),
            cachedClient.getSocketAddress(),
            cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - preResolveHost);
    final String resolveMsg = resolvedAddress.isUnresolved() ? "failed" : "succeed";
    if (hostResolveTimeMs > 2000) {
      logger.warn(
          "DNS resolution {} for {} took {} ms", resolveMsg, resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace(
          "DNS resolution {} for {} took {} ms", resolveMsg, resolvedAddress, hostResolveTimeMs);
    }

    // ? 为什么创建 client 要加锁
    // k 除了读，都要考虑锁
    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        // ? 为什么要再检查一次
        // k 可能 A B 线程都要创建新 client，防止重复创建
        if (cachedClient.isActive()) {
          logger.debug(
              "Returning cached connection from {} to {}: {}",
              cachedClient.getChannel().localAddress(),
              resolvedAddress,
              cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      clientPool.clients[clientIndex] = internalCreateClient(resolvedAddress, decoder);
      return clientPool.clients[clientIndex];
    }
  }

  public TransportClient createClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    return createClient(remoteHost, remotePort, -1);
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port. This
   * connection is not pooled.
   *
   * <p>As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException, InterruptedException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return internalCreateClient(address, NettyUtils.createFrameDecoder());
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port. This
   * connection is not pooled.
   *
   * <p>As with {@link #createClient(String, int)}, this method is blocking.
   */
  private TransportClient internalCreateClient(
      InetSocketAddress address, ChannelInboundHandlerAdapter decoder)
      throws IOException, InterruptedException {
    // k Bootstrap 是 Netty Channel 的创建器，通过 channel、eventloop、socket、pipeline 定义一个连接
    Bootstrap bootstrap = new Bootstrap();
    // k Channel 属于某个 EventLoopGroup 的某一个 EventLoop，EventLoop 负责处理 Channel 的 IO 事件，终生绑定。
    bootstrap
        .group(workerGroup)
        // ? 为什么要指定 Channel 实现类
        // k 选择 Channel 实现，如 NIO/Epoll，NIO 是 Java 标准实现，跨平台。 Epoll Linux 原生实现，开销更低。
        .channel(socketChannelClass)
        // Disable Nagle's Algorithm since we don't want packets to wait
        // ? 为什么要禁用 Nagle 算法
        // k 禁用小包合并，降低延迟。合并可以减少网络包，缺点是增加延迟。Celeborn 追求低延迟，禁用。
        .option(ChannelOption.TCP_NODELAY, true)
        // k 探测失效连接，探测太慢，不能替代 Celeborn 心跳
        .option(ChannelOption.SO_KEEPALIVE, true)
        // k 连接超时，单位毫秒，短则快速失败，长则容忍网络抖动
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
        // k 指定内存分配器，需定制。
        .option(ChannelOption.ALLOCATOR, allocator);

    if (receiveBuf > 0) {
      // ? 接受缓冲区大小如何设置？ 理论参考值 = 带宽 * RTT。
      // k 吞吐和抗突发能力 vs 内存与排队延迟
      // k 不配置则交给 OS 内核 autotune
      bootstrap.option(ChannelOption.SO_RCVBUF, receiveBuf);
    }

    if (sendBuf > 0) {
      bootstrap.option(ChannelOption.SO_SNDBUF, sendBuf);
    }

    // k 当 Netty 创建出真正的 SocketChannel 后，为它组装 Celeborn 的处理链，并取得与该 Channel 绑定的 TransportClient。
    // k 使用 AtomicReference 有两个原因：
    // k 1. Java 匿名内部类不能直接修改外层普通局部变量。
    // k 2. 初始化可能由 EventLoop 线程执行，外层调用线程负责读取，需要跨线程可见性。
    // k 这里它相当于一个线程安全的“返回值盒子”
    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();

    bootstrap.handler(
        // k bootstrap.connect 执行时，才会创建真的 Channel
        // k 重写，定制
        new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) {
            // k 每个 ch 都有自己的 pipeline，context 记录了一系列公共配置，用于组装定制 pipeline <gd>
            TransportChannelHandler clientHandler = context.initializePipeline(ch, decoder, true);
            clientRef.set(clientHandler.getClient());
          }
        });

    // Connect to the remote server
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (connectTimeoutMs <= 0) {
      awaitWithChannelCleanup(
          () -> {
            // k 不配置超时uuc时间，就一直阻塞
            cf.await();
            return true;
          },
          cf);
      assert cf.isDone();
      if (cf.isCancelled()) {
        closeChannel(cf);
        throw new IOException(String.format("Connecting to %s cancelled", address));
      } else if (!cf.isSuccess()) {
        closeChannel(cf);
        // k 失败原因有很多，放入异常链，上层处理。
        throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
      }
    } else if (!awaitWithChannelCleanup(() -> cf.await(connectTimeoutMs), cf)) {
      closeChannel(cf);
      throw new CelebornIOException(
          String.format("Connecting to %s timed out (%s ms)", address, connectTimeoutMs));
    } else if (cf.cause() != null) {
      // k 超时时间之内完成，但是结果失败
      closeChannel(cf);
      throw new CelebornIOException(String.format("Failed to connect to %s", address), cf.cause());
    }
    if (context.sslEncryptionEnabled()) {
      final SslHandler sslHandler = cf.channel().pipeline().get(SslHandler.class);
      sslHandler.setHandshakeTimeoutMillis(sslHandshakeTimeoutMs);
      Future<Channel> future =
          sslHandler
              .handshakeFuture()
              .addListener(
                  new GenericFutureListener<Future<Channel>>() {
                    @Override
                    public void operationComplete(final Future<Channel> handshakeFuture) {
                      if (handshakeFuture.isSuccess()) {
                        logger.debug("successfully completed TLS handshake to {}", address);
                      } else {
                        logger.info(
                            "failed to complete TLS handshake to {}",
                            address,
                            handshakeFuture.cause());
                        closeChannel(cf);
                      }
                    }
                  });
      if (!awaitWithChannelCleanup(() -> future.await(connectionTimeoutMs), cf)) {
        closeChannel(cf);
        throw new IOException(
            String.format("Failed to connect to %s within connection timeout", address));
      }
    }

    // k 此时检查是否 client 真正创建
    TransportClient client = clientRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    // k 这里的 bootstrap 与 Netty Bootstrap 不是一回事
    // k 这里可能会做：ASL 身份认证/客户端注册/交换身份信息/Master leader 确认
    long preBootstrap = System.nanoTime();
    logger.debug("Running bootstraps for {} ...", address);
    for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
      try {
        clientBootstrap.doBootstrap(client);
      } catch (
          Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
        // k Java 区分 checked exception 和 runtime Exception
        // k checked exception: 继承 Exception 但不继承 RuntimeExceptioIOE IOException。catch 必须声明 throws
        // k checked exception 需要考虑如何恢复正常

        // k RuntimeExcepiton: 例如 NullPointerException, IllegalArgumentException，编译器不强制处理
        // k checked exception 需要考虑 fix bug

        // k scala 不区分。
        long bootstrapTime = System.nanoTime() - preBootstrap;
        // k 对注册失败，日志强化
        if (clientBootstrap instanceof RegistrationClientBootstrap) {
          Exception processed = RegistrationClientBootstrap.processMasterNotLeaderException(e);
          String message =
              (processed instanceof MasterNotLeaderException)
                  ? String.format(
                      "Suggested leader is %s",
                      ((MasterNotLeaderException) processed).getSuggestedLeaderAddress())
                  : e.getMessage();
          logger.warn(
              "Attempted to register with a Master that is not the leader after {}: {}",
              Utils.nanoDurationToString(bootstrapTime),
              message);
        } else {
          logger.error(
              "Exception while bootstrapping client after {}",
              Utils.nanoDurationToString(bootstrapTime),
              e);
        }
        client.close();
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }
    long postBootstrap = System.nanoTime();
    logger.debug(
        "Successfully created connection to {} after {} ({} spent in bootstraps)",
        address,
        Utils.nanoDurationToString(postBootstrap - preConnect),
        Utils.nanoDurationToString(postBootstrap - preBootstrap));

    return client;
  }

  @FunctionalInterface
  @VisibleForTesting
  interface InterruptibleAwait {
    boolean await() throws InterruptedException;
  }

  @VisibleForTesting
  static boolean awaitWithChannelCleanup(
      InterruptibleAwait interruptibleAwait, ChannelFuture channelFuture)
      throws InterruptedException {
    try {
      return interruptibleAwait.await();
    } catch (InterruptedException e) {
      // k 当前线程收到 interrupt 标志，清理 ch，然后重新打上中断标记
      closeChannel(channelFuture);
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    // SPARK-19147
    if (workerGroup != null && !workerGroup.isShuttingDown()) {
      workerGroup.shutdownGracefully();
    }
  }

  public TransportContext getContext() {
    return context;
  }

  private static void closeChannel(ChannelFuture channelFuture) {
    try {
      channelFuture.channel().close();
    } catch (Exception e) {
      logger.warn("Failed to close channel", e);
    }
  }
}

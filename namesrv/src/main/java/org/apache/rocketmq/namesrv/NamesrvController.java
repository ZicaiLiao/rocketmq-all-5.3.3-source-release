/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.future.FutureTaskExt;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClientRequestProcessor;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.route.ZoneRouteRPCHook;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.Configuration;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.srvutil.FileWatchService;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 命名服务器控制器
 * 1. 启动时
 */
public class NamesrvController {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger WATER_MARK_LOG = LoggerFactory.getLogger(LoggerName.NAMESRV_WATER_MARK_LOGGER_NAME);

    /**
     * 命名服务器配置信息
     */
    private final NamesrvConfig namesrvConfig;

    /**
     * netty服务端配置新
     */
    private final NettyServerConfig nettyServerConfig;

    /**
     * netty 客户端配置信息
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * 周期性打印configTable、水位信息的线程池
     */
    private final ScheduledExecutorService scheduledExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScheduledThread").daemon(true).build());

    /**
     * 扫描不活跃broker的线程池
     */
    private final ScheduledExecutorService scanExecutorService = ThreadUtils.newScheduledThreadPool(1,
            new BasicThreadFactory.Builder().namingPattern("NSScanScheduledThread").daemon(true).build());

    /**
     * 键值对配置管理器
     */
    private final KVConfigManager kvConfigManager;

    /**
     * 路由信息管理器
     */
    private final RouteInfoManager routeInfoManager;

    /**
     * 远程客户端对象
     */
    private RemotingClient remotingClient;

    /**
     * 远程服务端对象
     */
    private RemotingServer remotingServer;

    /**
     * broker到namer sever是长连接，一旦连接关闭、连接发生异常、连接空置，
     * 则会从BrokerAddrInfo中移除该Broker的通道信息
     */
    private final BrokerHousekeepingService brokerHousekeepingService;

    /**
     * 默认线程池
     */
    private ExecutorService defaultExecutor;

    /**
     * 处理客户端请求线程池
     */
    private ExecutorService clientRequestExecutor;

    /**
     * 默认线程池的队列对象
     */
    private BlockingQueue<Runnable> defaultThreadPoolQueue;

    /**
     * 处理客户端请求的队列对象
     */
    private BlockingQueue<Runnable> clientRequestThreadPoolQueue;

    /**
     * 配置信息
     */
    private final Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this(namesrvConfig, nettyServerConfig, new NettyClientConfig());
    }

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.nettyClientConfig = nettyClientConfig;
        // 管理命名服务器配置信息的管理器
        this.kvConfigManager = new KVConfigManager(this);
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        // 管理路由信息的管理器【重点】
        this.routeInfoManager = new RouteInfoManager(namesrvConfig, this);
        this.configuration = new Configuration(LOGGER, this.namesrvConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    /**
     * 命名服务控制器的初始化
     * 1. 加载配置信息
     * 2. 初始化网络组件
     * 3. 初始化线程池
     * 4. 注册处理器
     * 5. 启动调度服务
     * 6. 初始化SSL上下文
     * 7. 初始化RPC回调
     *
     * @return 是否初始化成功
     */
    public boolean initialize() {
        // 加载配置信息
        loadConfig();
        // 初始化网络组件
        initiateNetworkComponents();
        // 初始化线程池
        initiateThreadExecutors();
        // 注册处理器
        registerProcessor();
        // 启动调度服务
        startScheduleService();
        // 初始化SSL上下文
        initiateSslContext();
        // 初始化RPC回调
        initiateRpcHooks();
        return true;
    }

    /**
     * 从配置文件加载命名服务器的配置信息
     */
    private void loadConfig() {
        this.kvConfigManager.load();
    }

    /**
     * 启动调度服务：
     *
     */
    private void startScheduleService() {
        // 使用扫描线程池调度扫描不活跃broker，延迟5毫秒开始执行，默认每隔5秒执行一次
        this.scanExecutorService.scheduleAtFixedRate(NamesrvController.this.routeInfoManager::scanNotActiveBroker,
            5, this.namesrvConfig.getScanNotActiveBrokerInterval(), TimeUnit.MILLISECONDS);

        // 使用调度线程池周期性打印configTable中的配置信息，延迟1分钟开始执行，每隔10分钟打印一次
        this.scheduledExecutorService.scheduleAtFixedRate(NamesrvController.this.kvConfigManager::printAllPeriodically,
            1, 10, TimeUnit.MINUTES);

        // 固定时间间隔调度执行打印水位信息，延迟10秒开始执行，每隔1秒钟执行一次
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                NamesrvController.this.printWaterMark();
            } catch (Throwable e) {
                LOGGER.error("printWaterMark error.", e);
            }
        }, 10, 1, TimeUnit.SECONDS);
    }

    /**
     * 初始化网络组件
     */
    private void initiateNetworkComponents() {
        /*
            创建netty远程服务器对象，设置netty服务器的配置信息
                - publicExecutor 线程默认为4个
                - scheduledExecutor、bossExecutor线程默认都为1
                - selector线程默认为3个
         */
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

        /*
            创建netty远程客户端对象，设置netty客户端配置信息
                - publicExecutor 线程默认为4个
                - scanExecutor 线程池核心线程数为4，最大线程数为10，queue为32
                - eventLoopGroupWorker 线程数为1
         */
        this.remotingClient = new NettyRemotingClient(this.nettyClientConfig);
    }

    /**
     * 初始化线程池，包含两个线程
     * 1. 默认线程池
     * 2. 客户端请求线程池
     */
    private void initiateThreadExecutors() {
        // 默认线程池队列对象，线程池队列大小默认为 1W，用于暂存broker请求或操作请求
        this.defaultThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getDefaultThreadPoolQueueCapacity());
        // 创建默认线程池对象，默认核心线程数为16，最大线程数默认为16，空闲线程空闲时长为60s
        this.defaultExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getDefaultThreadPoolNums(), this.namesrvConfig.getDefaultThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.defaultThreadPoolQueue, new ThreadFactoryImpl("RemotingExecutorThread_"));

        // 创建客户端请求线程队列，线程池队列大小默认为 5W
        this.clientRequestThreadPoolQueue = new LinkedBlockingQueue<>(this.namesrvConfig.getClientRequestThreadPoolQueueCapacity());
        // 创建客户端请求线程池对象，默认核心线程数为8，最大线程数为8，空闲线程空闲时长为60s
        this.clientRequestExecutor = ThreadUtils.newThreadPoolExecutor(this.namesrvConfig.getClientRequestThreadPoolNums(), this.namesrvConfig.getClientRequestThreadPoolNums(), 1000 * 60, TimeUnit.MILLISECONDS, this.clientRequestThreadPoolQueue, new ThreadFactoryImpl("ClientRequestExecutorThread_"));
    }

    private void initiateSslContext() {
        if (TlsSystemConfig.tlsMode == TlsMode.DISABLED) {
            return;
        }

        String[] watchFiles = {TlsSystemConfig.tlsServerCertPath, TlsSystemConfig.tlsServerKeyPath, TlsSystemConfig.tlsServerTrustCertPath};

        FileWatchService.Listener listener = new FileWatchService.Listener() {
            boolean certChanged, keyChanged = false;

            @Override
            public void onChanged(String path) {
                // 如果传递的路径是服务器信任证书路径，则加载SSL上下文
                if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                    LOGGER.info("The trust certificate changed, reload the ssl context");
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
                // 如果传递的路径是服务器证书路径，则重新加载SSL上下文
                if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                    certChanged = true;
                }
                // 如果传递的路径是服务器私钥路径，则重新加载SSL上下文
                if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                    keyChanged = true;
                }
                // TTS服务器的私钥文件和证书文件同时发生改变，则重新加载SSL上下文
                if (certChanged && keyChanged) {
                    LOGGER.info("The certificate and private key changed, reload the ssl context");
                    certChanged = keyChanged = false;
                    ((NettyRemotingServer) remotingServer).loadSslContext();
                }
            }
        };

        try {
            fileWatchService = new FileWatchService(watchFiles, listener);
        } catch (Exception e) {
            LOGGER.warn("FileWatchService created error, can't load the certificate dynamically");
        }
    }

    private void printWaterMark() {
        WATER_MARK_LOG.info("[WATERMARK] ClientQueueSize:{} ClientQueueSlowTime:{} " + "DefaultQueueSize:{} DefaultQueueSlowTime:{}", this.clientRequestThreadPoolQueue.size(), headSlowTimeMills(this.clientRequestThreadPoolQueue), this.defaultThreadPoolQueue.size(), headSlowTimeMills(this.defaultThreadPoolQueue));
    }

    private long headSlowTimeMills(BlockingQueue<Runnable> q) {
        long slowTimeMills = 0;
        final Runnable firstRunnable = q.peek();

        if (firstRunnable instanceof FutureTaskExt) {
            final Runnable inner = ((FutureTaskExt<?>) firstRunnable).getRunnable();
            if (inner instanceof RequestTask) {
                slowTimeMills = System.currentTimeMillis() - ((RequestTask) inner).getCreateTimestamp();
            }
        }

        if (slowTimeMills < 0) {
            slowTimeMills = 0;
        }

        return slowTimeMills;
    }

    /**
     * 注册处理器，将请求码、请求处理器、执行请求处理的线程池添加到远程服务端
     */
    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.defaultExecutor);
        } else {
            // Support get route info only temporarily
            // 客户端请求处理器，目前仅支持获取路由信息请求的处理
            ClientRequestProcessor clientRequestProcessor = new ClientRequestProcessor(this);
            // 将请求码、请求处理器以及执行请求处理的线程池注册到NettyRemotingServer中，本质上是添加到processorTable这个hashmap中
            this.remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, clientRequestProcessor, this.clientRequestExecutor);

            // 注册默认请求处理器，使用默认线程池进行处理请求
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.defaultExecutor);
        }
    }

    private void initiateRpcHooks() {
        this.remotingServer.registerRPCHook(new ZoneRouteRPCHook());
    }

    public void start() throws Exception {
        // 启动NettyRemotingServer
        this.remotingServer.start();

        // In test scenarios where it is up to OS to pick up an available port, set the listening port back to config
        if (0 == nettyServerConfig.getListenPort()) {
            nettyServerConfig.setListenPort(this.remotingServer.localListenPort());
        }

        // 给远程客户端更新命名服务器地址列表，IP地址是本地地址，端口是netty服务端的监听端口
        this.remotingClient.updateNameServerAddressList(Collections.singletonList(NetworkUtil.getLocalAddress()
            + ":" + nettyServerConfig.getListenPort()));
        // 启动远程客户端
        this.remotingClient.start();

        // 如果设置了FileWatchService，则启动FileWatchService
        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }

        // 启动路由信息管理器【重要】这里有几个表用于记录元数据信息
        this.routeInfoManager.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
        this.remotingServer.shutdown();
        this.defaultExecutor.shutdown();
        this.clientRequestExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.scanExecutorService.shutdown();
        this.routeInfoManager.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public RemotingClient getRemotingClient() {
        return remotingClient;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}

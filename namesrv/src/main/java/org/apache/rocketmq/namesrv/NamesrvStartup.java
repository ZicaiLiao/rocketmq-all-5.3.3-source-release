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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.ControllerConfig;
import org.apache.rocketmq.common.JraftConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.controller.ControllerManager;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.srvutil.ShutdownHookThread;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;

public class NamesrvStartup {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private static final Logger logConsole = LoggerFactory.getLogger(LoggerName.NAMESRV_CONSOLE_LOGGER_NAME);
    private static Properties properties = null;
    private static NamesrvConfig namesrvConfig = null;
    private static NettyServerConfig nettyServerConfig = null;
    private static NettyClientConfig nettyClientConfig = null;
    private static ControllerConfig controllerConfig = null;

    public static void main(String[] args) {
        main0(args);
        // 该方法的代码逻辑不会被执行到，可以忽略
        controllerManagerMain();
    }

    public static NamesrvController main0(String[] args) {
        try {
            // RocketMQ命令服务器的启动类，主要作用是解析命令行参数和配置文件的参数，并根据这些参数配置NameServer的运行环境
            parseCommandlineAndConfigFile(args);
            // 创建并启动NameServer的控制器，然后返回控制器对象，以便于后续管理和操作
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            // 添加异常退出码
            System.exit(-1);
        }

        return null;
    }

    public static ControllerManager controllerManagerMain() {
        try {
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                return createAndStartControllerManager();
            }
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return null;
    }

    /**
     * RocketMQ 命名服务器的启动类，主要作用是解析命令行参数和配置文件中的参数，并根据这些参数配置NameServer的运行环境。
     * <p>
     * 主要完成以下任务：
     * <ol>
     *     <li> 设置系统参数：设置`rocketmq.remoting.version`为`V5_3_3`。
     *     <li> 创建保存选项值的对象：创建一个`Options`对象，用于保存命令行参数和配置文件中的选项值。
     *     <li> 解析命令行参数：调用`ServerUtil.parseCmdLine()`方法解析命令行参数，将解析结果封装到`CommandLine`对象中。
     *     <li> 解析配置文件：如果命令行参数指定了配置文件，则解析配置文件中的参数，并将其添加到相应的配置对象中。
     *     <li> 解析命令行参数中的命名服务器配置：将命令行参数中的命名服务器配置信息添加到`NamesrvConfig`对象中。
     *     <li> 解析命令行参数中的Netty服务端配置：将命令行参数中的Netty服务端配置信息添加到`NettyServerConfig`对象中。
     *     <li> 解析命令行参数中的Netty客户端配置：将命令行参数中的Netty客户端配置信息添加到`NettyClientConfig`对象中。
     *     <li> 解析命令行参数中的控制器配置：如果当前命名服务器启用了控制器，则解析控制器的配置信息，并将其添加到`ControllerConfig`对象中。
     *     <li> 打印配置信息：如果命令行参数指定了`-p`选项，则打印命名服务器、Netty服务端和Netty客户端的配置信息，以及控制器的配置信息（如果启用了控制器）。
     *     <li> 检查环境变量：检查`ROCKETMQ_HOME`环境变量是否设置正确，如果不设置或设置错误，则打印错误信息并退出。
     * </ol>
     *
     * @param args 命令行参数数组
     *
     * @throws Exception 异常信息
     */
    public static void parseCommandlineAndConfigFile(String[] args) throws Exception {
        // 设置系统参数，key为rocketmq.remoting.version，value为V5_3_3（也就是当前rocketmq的版本）
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        // 创建保存选项值的对象，默认创建了help、namesrvAddr options
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        /*
            从命令行参数args中，解析出Options中对应选项的值，封装到CommandLine对象中并返回
            params：
                appName - 应用名称，NameServer的名称为mqnamesrv
                args    - 命令行参数数组
                options - 封装了该应用支持的命令行选项的Options对象
                parser  - 用于解析命令行参数的CommandLineParser对象
         */
        CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options), new DefaultParser());
        // 如果解析出错，返回值为null
        if (null == commandLine) {
            System.exit(-1);
            return;
        }

        // 用于封装NameServer的配置参数
        namesrvConfig = new NamesrvConfig();
        // 用于封装NettyServer配置信息的对象
        nettyServerConfig = new NettyServerConfig();
        // 用于封装NettyClient配置信息的对象
        nettyClientConfig = new NettyClientConfig();
        // 设置netty默认的端口为9876，也就是说NameServer默认的端口为9876
        nettyServerConfig.setListenPort(9876);
        // 如果命令行参数设置了c，代表指定了配置文件，需要进行解析
        if (commandLine.hasOption('c')) {
            // 获取配置文件的路径
            String file = commandLine.getOptionValue('c');
            if (file != null) {
                InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(file)));
                properties = new Properties();
                // 加载配置文件信息
                properties.load(in);

                // 从properties中解析出命名服务器的配置信息，并添加到nemesrvConfig对象中
                MixAll.properties2Object(properties, namesrvConfig);
                // 从properties中解析出netty服务端的配置信息，并添加到nettyServerConfig中
                MixAll.properties2Object(properties, nettyServerConfig);
                // 从properties中解析出netty客户端到配置信息，并封装到nettyClientConfig中
                MixAll.properties2Object(properties, nettyClientConfig);

                // 如果需要在当前命名服务器中开启控制器
                if (namesrvConfig.isEnableControllerInNamesrv()) {
                    // 创建控制器配置对象
                    controllerConfig = new ControllerConfig();
                    JraftConfig jraftConfig = new JraftConfig();
                    controllerConfig.setJraftConfig(jraftConfig);
                    // 从properties中解析出控制器的配置信息，并添加到控制器配置对象中
                    MixAll.properties2Object(properties, controllerConfig);
                    // 从properties中解析出jRaft的配置信息，并添加到jRaft配置对象中
                    MixAll.properties2Object(properties, jraftConfig);
                }
                // 指定配置文件路径
                namesrvConfig.setConfigStorePath(file);

                // 打印加载控制器配置信息完成
                System.out.printf("load config properties file OK, %s%n", file);
                // 关闭配置文件的输入流
                in.close();
            }
        }

        // 从命令行中解析出与命名服务器相关的参数，添加到namesrvConfig对象中
        MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
        // 如果命令行参数包含了p，表示需要打印配置信息
        if (commandLine.hasOption('p')) {
            // 打印命名服务器配置信息
            MixAll.printObjectProperties(logConsole, namesrvConfig);
            // 打印netty服务端配置信息
            MixAll.printObjectProperties(logConsole, nettyServerConfig);
            // 打印netty客户端配置信息
            MixAll.printObjectProperties(logConsole, nettyClientConfig);
            // 如果在当前命名服务器启动了控制器，则打印对应控制器的信息
            if (namesrvConfig.isEnableControllerInNamesrv()) {
                MixAll.printObjectProperties(logConsole, controllerConfig);
            }
            // 当前JVM进程退出
            System.exit(0);
        }

        // 如果没有指定环境变量ROCKETMQ_HOME，则打印错误信息，程序退出
        if (null == namesrvConfig.getRocketmqHome()) {
            System.out.printf("Please set the %s variable in your environment to match the location of the RocketMQ installation%n", MixAll.ROCKETMQ_HOME_ENV);
            System.exit(-2);
        }
        // 打印命名服务器配置信息
        MixAll.printObjectProperties(log, namesrvConfig);
        // 打印netty服务端配置信息
        MixAll.printObjectProperties(log, nettyServerConfig);

    }

    /**
     * 该方法主要作用是创建并启动NameServer的控制器，然后返回控制器对象，以便后续管理和操作。<br>
     *
     * <ol>
     *   <li>调用`createNamesrvController()`方法创建一个NameServer的控制器对象。</li>
     *   <li>调用`start()`方法启动控制器对象，即启动NameServer。</li>
     *   <li>获取控制器的Netty服务端配置对象，以便获取服务端绑定的地址和端口。</li>
     *   <li>打印成功启动NameServer的信息，包括序列化类型、绑定地址和监听端口。</li>
     *   <li>返回控制器对象。</li>
     * </ol>
     *
     * @return 命名控制器对象
     *
     * @throws Exception 异常信息
     */
    public static NamesrvController createAndStartNamesrvController() throws Exception {

        // 使用命名服务器配置信息、netty服务端配置信息、netty客户端配置信息进行创建NamesrvController对象，并将配置信息保存到控制器中，防止丢失
        // 返回命名控制器对象
        NamesrvController controller = createNamesrvController();
        // 启动命名服务控制器
        start(controller);
        NettyServerConfig serverConfig = controller.getNettyServerConfig();
        String tip = String.format("The Name Server boot success. serializeType=%s, address %s:%d", RemotingCommand.getSerializeTypeConfigInThisServer(), serverConfig.getBindAddress(), serverConfig.getListenPort());
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controller;
    }

    /**
     * 使用命名服务器配置信息、netty服务端配置信息、netty客户端配置信息进行创建NamesrvController对象，并将配置信息保存到控制器中，防止丢失
     *
     * @return 命名服务器控制器
     */
    public static NamesrvController createNamesrvController() {

        // 使用命名服务器配置信息、netty服务端配置信息、netty客户端配置信息进行创建NamesrvController对象
        final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        // 将配置信息记录到控制器对象中防止被丢弃
        controller.getConfiguration().registerConfig(properties);
        return controller;
    }

    /**
     * 启动命名服务控制器
     *
     * @param controller 命名服务控制器
     *
     * @return 命名服务控制器
     *
     * @throws Exception 异常信息
     */
    public static NamesrvController start(final NamesrvController controller) throws Exception {

        if (null == controller) {
            throw new IllegalArgumentException("NamesrvController is null");
        }

        // 控制器初始化
        boolean initResult = controller.initialize();
        // 如果初始化失败，则程序异常退出
        if (!initResult) {
            controller.shutdown();
            System.exit(-3);
        }

        /*
            给运行时添加关闭回调，当运行关闭的时候，先关闭NameServerController，再运行程序退出
            eg：
                - kill <pid> 这时候会执行回调函数
                - kill -9 <pid> 不会执行回调函数
         */
        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controller.shutdown();
            return null;
        }));

        // 启动命名控制器
        controller.start();

        return controller;
    }

    public static ControllerManager createAndStartControllerManager() throws Exception {
        ControllerManager controllerManager = createControllerManager();
        start(controllerManager);
        String tip = "The ControllerManager boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
        log.info(tip);
        System.out.printf("%s%n", tip);
        return controllerManager;
    }

    public static ControllerManager createControllerManager() throws Exception {
        NettyServerConfig controllerNettyServerConfig = (NettyServerConfig) nettyServerConfig.clone();
        ControllerManager controllerManager = new ControllerManager(controllerConfig, controllerNettyServerConfig, nettyClientConfig);
        // remember all configs to prevent discard
        controllerManager.getConfiguration().registerConfig(properties);
        return controllerManager;
    }

    public static ControllerManager start(final ControllerManager controllerManager) throws Exception {

        if (null == controllerManager) {
            throw new IllegalArgumentException("ControllerManager is null");
        }

        boolean initResult = controllerManager.initialize();
        if (!initResult) {
            controllerManager.shutdown();
            System.exit(-3);
        }

        Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(log, (Callable<Void>) () -> {
            controllerManager.shutdown();
            return null;
        }));

        controllerManager.start();

        return controllerManager;
    }

    public static void shutdown(final NamesrvController controller) {
        controller.shutdown();
    }

    public static void shutdown(final ControllerManager controllerManager) {
        controllerManager.shutdown();
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config items");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    public static Properties getProperties() {
        return properties;
    }
}

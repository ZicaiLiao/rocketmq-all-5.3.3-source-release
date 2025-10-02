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
package org.apache.rocketmq.srvutil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Properties;

public class ServerUtil {

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt =
            new Option("n", "namesrvAddr", true,
                "Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * 从命令行参数args中，解析出Options中对应选项的值，封装到CommandLine对象并返回
     *
     * @param appName 应用名称，NameServer的名称就是“mqnamesrv”
     * @param args    命令行参数数组
     * @param options 封装了该应用支持的命令行选项的Options对象
     * @param parser  用于解析命令行参数的CommandLineParser对象
     *
     * @return commandline对象
     */
    public static CommandLine parseCmdLine(final String appName, String[] args, Options options,
        CommandLineParser parser) {
        // 创建用于打印帮助信息的格式化对象
        HelpFormatter hf = new HelpFormatter();
        // 设置打印帮助信息的行字符长度
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            // 用于从命令行参数args中，解析出options中设置好的选项信息的值，封装到CommandLine对象中，底层用的是反射
            commandLine = parser.parse(options, args);
            // 如果命令行参数中包含h参数，表示打印命令参数信息，程序正常退出
            if (commandLine.hasOption('h')) {
                // 打印命令的参数信息
                hf.printHelp(appName, options, true);
                // 程序正常退出
                System.exit(0);
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            hf.printHelp(appName, options, true);
            System.exit(1);
        }

        // 返回解析好的commandline对象
        return commandLine;
    }

    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}

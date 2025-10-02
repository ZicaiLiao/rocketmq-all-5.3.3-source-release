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

import com.google.common.base.Strings;
import org.apache.rocketmq.common.LifecycleAwareServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

/**
 * 主要用于监控NameServer和Broker的信任证书文件，认证文件以及私钥文件的更改。
 */
public class FileWatchService extends LifecycleAwareServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final Map<String, String> currentHash = new HashMap<>();
    private final Listener listener;
    private static final int WATCH_INTERVAL = 500;
    private final MessageDigest md = MessageDigest.getInstance("MD5");

    public FileWatchService(final String[] watchFiles,
        final Listener listener) throws Exception {
        // 设置监听器，对需要监听的文件进行监听
        this.listener = listener;
        // 遍历需要监听的文件，并计算出文件的MD5值，存入到map中
        for (String file : watchFiles) {
            if (!Strings.isNullOrEmpty(file) && new File(file).exists()) {
                currentHash.put(file, md5Digest(file));
            }
        }
    }

    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    @Override
    public void run0() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                // 等待500ms
                this.waitForRunning(WATCH_INTERVAL);
                // 遍历需要监听的文件集合
                for (Map.Entry<String, String> entry : currentHash.entrySet()) {
                    // 根据文件对象计算MD5值
                    String newHash = md5Digest(entry.getKey());
                    // 将计算出来的MD5值与保存的MD5值进行对比，如果不同，则将计算出来的MD5值更新到map中，
                    // 同时触发该文件的监听器onChange方法进行文件更改处理
                    if (!newHash.equals(entry.getValue())) {
                        entry.setValue(newHash);
                        listener.onChanged(entry.getKey());
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service raised an unexpected exception.", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * Note: we ignore DELETE event on purpose. This is useful when application renew CA file.
     * When the operator delete/rename the old CA file and copy a new one, this ensures the old CA file is used during
     * the operation.
     * <p>
     * As we know exactly what to do when file does not exist or when IO exception is raised, there is no need to
     * propagate the exception up.
     *
     * @param filePath Absolute path of the file to calculate its MD5 digest.
     * @return Hash of the file content if exists; empty string otherwise.
     */
    private String md5Digest(String filePath) {
        Path path = Paths.get(filePath);
        if (!path.toFile().exists()) {
            // Reuse previous hash result
            return currentHash.getOrDefault(filePath, "");
        }
        byte[] raw;
        try {
            raw = Files.readAllBytes(path);
        } catch (IOException e) {
            log.info("Failed to read content of {}", filePath);
            // Reuse previous hash result
            return currentHash.getOrDefault(filePath, "");
        }
        md.update(raw);
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         *
         * @param path the changed file path
         */
        void onChanged(String path);
    }
}

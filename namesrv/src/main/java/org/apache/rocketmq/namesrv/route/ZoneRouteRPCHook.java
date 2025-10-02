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
package org.apache.rocketmq.namesrv.route;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 这是一个 Java 类，名为 `ZoneRouteRPCHook`，实现了 `RPCHook` 接口。
 * 该类的主要目的是对消息路由进行过滤，以便在特定的zone模式下仅返回该区域的数据。
 * <p>
 * 在 `doAfterResponse` 方法中，首先判断请求的代码和响应是否为空，以及响应的代码是否为成功代码。
 * 如果条件不满足，则不会进行任何操作。
 * <p>
 * 如果条件满足，则将响应解码为 `TopicRouteData` 对象，并调用 `filterByZoneName` 方法进行过滤。
 * 该方法会根据请求中的区域名称对路由数据进行过滤，只保留该区域的数据。
 * <p>
 * 过滤后，将修改后的 `TopicRouteData` 对象重新编码并设置回响应对象中，然后返回该响应。
 */
public class ZoneRouteRPCHook implements RPCHook {

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {

    }

    /**
     * 对于获取主题路由信息的请求，在处理器处理完之后，对结果进行zone模式的过滤。
     * 如果请求不是zone模式，则不过滤。
     *
     * @param remoteAddr 远程地址
     * @param request    远程进来的请求
     * @param response   处理器生成的响应
     */
    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        // 如果不是获取主题路由信息的请求，则直接返回
        if (RequestCode.GET_ROUTEINFO_BY_TOPIC != request.getCode()) {
            return;
        }
        // 如果没有响应或者数据为空，直接返回
        if (response == null || response.getBody() == null || ResponseCode.SUCCESS != response.getCode()) {
            return;
        }
        // 如果zone名称为空，则直接返回
        boolean zoneMode = Boolean.parseBoolean(request.getExtFields().get(MixAll.ZONE_MODE));
        if (!zoneMode) {
            return;
        }
        // 如果zone名称为空，则直接返回
        String zoneName = request.getExtFields().get(MixAll.ZONE_NAME);
        if (StringUtils.isBlank(zoneName)) {
            return;
        }
        // 首先将响应中的主题路由信息进行解码
        TopicRouteData topicRouteData = RemotingSerializable.decode(response.getBody(), TopicRouteData.class);
        // 根据zone名称对主题路由信息进行过滤并编码，设置给响应
        response.setBody(filterByZoneName(topicRouteData, zoneName).encode());
    }

    /**
     * 根据zone名称对主题路由信息进行过滤
     *
     * @param topicRouteData 主题路由信息
     * @param zoneName       zone名称
     *
     * @return 过滤后的主题路由信息
     */
    private TopicRouteData filterByZoneName(TopicRouteData topicRouteData, String zoneName) {
        List<BrokerData> brokerDataReserved = new ArrayList<>();
        Map<String, BrokerData> brokerDataRemoved = new HashMap<>();
        for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
            if (brokerData.getBrokerAddrs() == null) {
                continue;
            }
            //master down, consume from slave. break nearby route rule.
            if (brokerData.getBrokerAddrs().get(MixAll.MASTER_ID) == null
                || StringUtils.equalsIgnoreCase(brokerData.getZoneName(), zoneName)) {
                brokerDataReserved.add(brokerData);
            } else {
                brokerDataRemoved.put(brokerData.getBrokerName(), brokerData);
            }
        }
        topicRouteData.setBrokerDatas(brokerDataReserved);

        List<QueueData> queueDataReserved = new ArrayList<>();
        for (QueueData queueData : topicRouteData.getQueueDatas()) {
            if (!brokerDataRemoved.containsKey(queueData.getBrokerName())) {
                queueDataReserved.add(queueData);
            }
        }
        topicRouteData.setQueueDatas(queueDataReserved);
        // remove filter server table by broker address
        if (topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            for (Entry<String, BrokerData> entry : brokerDataRemoved.entrySet()) {
                BrokerData brokerData = entry.getValue();
                brokerData.getBrokerAddrs().values()
                    .forEach(brokerAddr -> topicRouteData.getFilterServerTable().remove(brokerAddr));
            }
        }
        return topicRouteData;
    }
}

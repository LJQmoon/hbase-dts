/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-6 下午8:12
 */

package com.gsww.healthplatform.hbasedts.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.gsww.healthplatform.hbasedts.utils.RefCount;
import org.apache.http.HttpHost;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ES连接管理器
 */
public class ESConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ESConnectionManager.class);
    private static ESConnectionManager ourInstance = new ESConnectionManager();

    private Map<String, RefCount<RestHighLevelClient>> clientMap = new HashMap<>();

    public static ESConnectionManager getInstance() {
        return ourInstance;
    }

    private ESConnectionManager() {
    }

    public synchronized RestHighLevelClient getConnection(HttpHost... hosts) {
        Preconditions.checkState(hosts != null && hosts.length>0);
        // 判断ES连接是否存在
        String hostsKey = hosts2Key(hosts);
        RefCount<RestHighLevelClient> clientRef = clientMap.get(hostsKey);
        if (clientRef != null) {
            clientRef.addRef();
            return clientRef.get();
        }
        // 创建新的ES连接
        RestClientBuilder clientBuilder = RestClient.builder(hosts)
                .setFailureListener(new RestClient.FailureListener() {
                    @Override
                    public void onFailure(Node node) {
                        logger.error("Failure node: {}", node.toString());
                        super.onFailure(node);
                    }
                })
                .setMaxRetryTimeoutMillis(10000)
                .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);
        RestHighLevelClient client = new RestHighLevelClient(clientBuilder);
        // 缓存ES连接
        clientMap.put(hostsKey, new RefCount<>(client));
        return client;
    }

    private String hosts2Key(HttpHost... hosts) {
        List<String> hostList = Lists.newArrayList();
        for (HttpHost host : hosts) {
            hostList.add(String.format("%s:%d", host.getHostName(), host.getPort()));
        }
        StringBuilder keyBuilder = new StringBuilder();
        for (String s : Ordering.natural().sortedCopy(hostList)) {
            keyBuilder.append(s);
        }
        return keyBuilder.toString();
    }

    public synchronized void releaseConnection(HttpHost... hosts) {
        String hostsKey = hosts2Key(hosts);
        RefCount<RestHighLevelClient> clientRef = clientMap.get(hostsKey);
        if (clientRef != null) {
            if (clientRef.release() <= 0) {
                // 释放连接
                clientMap.remove(hostsKey);
                try {
                    RestClient lowLevelClient = clientRef.get().getLowLevelClient();
                    lowLevelClient.close();
                    clientRef.get().close();
                } catch (IOException e) {
                    logger.error("close RestHighLevelClient", e);
                }
            }
        }
    }
}

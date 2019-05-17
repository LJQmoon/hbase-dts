/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-5 下午8:34
 */

package com.gsww.healthplatform.hbasedts.elasticsearch.sink;

import com.gsww.healthplatform.hbasedts.arch.Event;
import com.gsww.healthplatform.hbasedts.arch.base.AbstractSink;
import com.gsww.healthplatform.hbasedts.arch.base.KeyPattern;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleState;
import com.gsww.healthplatform.hbasedts.elasticsearch.ESConnectionManager;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class ElasticsearchSink extends AbstractSink implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSink.class);

    private HttpHost[] hosts;
    private String index;
    private String type;
    private KeyPattern idPattern;
    private BulkProcessor bulkProcessor;
    private RestHighLevelClient client;
    private long lastBeforeBulkId = Long.MIN_VALUE; // 最新的Bulk请求ID
    private long lastAfterBulkId = Long.MIN_VALUE;  // 最新的Bulk完成ID

    public ElasticsearchSink(HttpHost[] hosts, String index, String type, KeyPattern idPattern) {
        this.hosts = hosts;
        this.index = index;
        this.type = type;
        this.idPattern = idPattern;
    }

    @Override
    public void start() {
        initClient();
        // 启动数据写入线程
        new Thread(this).start();
        super.start();
    }

    // 初始化连接
    private void initClient() {
        client = ESConnectionManager.getInstance().getConnection(hosts);
        BulkProcessor.Listener bulkListener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                lastBeforeBulkId = l;
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                lastAfterBulkId = l;
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                lastAfterBulkId = l;
                logger.error(String.format("Elasticsearch sink failure (index: %s, type: %s)", index, type), throwable);
            }
        };
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = new BiConsumer<BulkRequest, ActionListener<BulkResponse>>() {
            @Override
            public void accept(BulkRequest bulkRequest, ActionListener<BulkResponse> bulkResponseActionListener) {
                client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener);
            }
        };
        bulkProcessor = BulkProcessor.builder(bulkConsumer, bulkListener)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(200), 32))
//                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
//                .setFlushInterval(new TimeValue(5, TimeUnit.SECONDS))
                .build();
    }

    // 释放连接
    private void releaseClient() {
        // 等待批处理任务执行完成
        while (lastBeforeBulkId != lastAfterBulkId) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        // 释放连接
        if (client != null) {
            ESConnectionManager.getInstance().releaseConnection(hosts);
            client = null;
        }
    }

    @Override
    public void run() {
        int count = 0;
        long startTime = System.currentTimeMillis();
        Event event;
        do {
            event = channel.take();
            if (event != null) {
                JSONObject obj = event2Json(event);
                if (idPattern != null) {
                    bulkProcessor.add(new IndexRequest(index, type, idPattern.format(event)).source(obj.toString(), XContentType.JSON));
                } else {
                    bulkProcessor.add(new IndexRequest(index, type).source(obj.toString(), XContentType.JSON));
                }
                count++;
            }
        } while (event != null && state == LifecycleState.START);
        // 提交最后的数据
        bulkProcessor.flush();
        releaseClient();
        stop();
        logger.info("Elasticsearch sink ({}, {}) imported {} objects; speed: {}ops.", index, type, count,
                count/Math.max((System.currentTimeMillis()-startTime)/1000, 1));
    }

    private JSONObject event2Json(Event event) {
        JSONObject result = new JSONObject();
        for (String key : event.getKeys()) {
            try {
				result.put(key, event.get(key));
			} catch (JSONException e) {
				e.printStackTrace();
			}
        }
        return result;
    }
}

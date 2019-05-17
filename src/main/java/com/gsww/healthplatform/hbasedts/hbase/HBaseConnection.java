/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Component
public class HBaseConnection {
    @Value("${hbase.zk-hosts}")
    private String zkHosts;

    @Value("${hbase.zk-port}")
    private String zkPort;

    private Configuration conf;
    private Connection connection;

    @PostConstruct
    protected void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkHosts);
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort);
        connection = ConnectionFactory.createConnection(conf);
    }

    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }
}

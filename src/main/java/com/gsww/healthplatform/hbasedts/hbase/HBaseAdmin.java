/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.hbase;

import com.gsww.healthplatform.hbasedts.hbase.write.HFamilyDesc;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class HBaseAdmin {
    @Autowired
    private HBaseConnection hbaseConnection;

    public void createNamespace(String namespace) throws IOException {
        try (Admin admin = hbaseConnection.getAdmin()) {
            admin.createNamespace(NamespaceDescriptor.create(namespace).build());
        }
    }

    public void dropNamespace(String namespace) throws IOException {
        try (Admin admin = hbaseConnection.getAdmin()) {
            admin.deleteNamespace(namespace);
        }
    }

    // HBase client version 2.1
//    public void createTable(String tableName, HFamilyDesc[] familys) throws IOException {
//        try (Admin admin = hbaseConnection.getAdmin()) {
//            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
//            for (HFamilyDesc family : familys) {
//                tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family.getFamilyName()))
//                        .setBlocksize(family.getBlockSize())
//                        .build());
//            }
//            admin.createTable(tableDescriptorBuilder.build());
//        }
//    }

    // HBase client version 1.2.0
    public void createTable(String tableName, HFamilyDesc[] familys) throws IOException {
        try (Admin admin = hbaseConnection.getAdmin()) {
//            if (admin.tableExists(TableName.valueOf(tableName))) {
//                throw new TableExistsException(tableName);
//            }
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
            table.addFamily(new HColumnDescriptor(Bytes.toBytes("MINOR")).setBlocksize(64 * 1024));
            for (HFamilyDesc family : familys) {
                table.addFamily(new HColumnDescriptor(Bytes.toBytes(family.getFamilyName())).setBlocksize(family.getBlockSize()));
            }
            admin.createTable(table);
        }
    }

    public void dropTable(String tableName) throws IOException {
        try (Admin admin = hbaseConnection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(tableName);
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        }
    }
}

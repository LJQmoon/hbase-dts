/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.hbase.sink;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.gsww.healthplatform.hbasedts.arch.Event;
import com.gsww.healthplatform.hbasedts.arch.base.AbstractSink;
import com.gsww.healthplatform.hbasedts.arch.base.KeyPattern;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleState;
import com.gsww.healthplatform.hbasedts.hbase.HBaseAdmin;
import com.gsww.healthplatform.hbasedts.hbase.HBaseConnection;
import com.gsww.healthplatform.hbasedts.hbase.write.HFamilyDesc;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HBaseSink extends AbstractSink implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HBaseSink.class);
    private static final int BULK_SIZE = 10000;

    private ConcurrentLinkedQueue<List<Put>> putsQueue = new ConcurrentLinkedQueue();

    private HBaseConnection connection;
    private HBaseAdmin admin;

    private int putThreadCount;
    private String tableName;
    private HFamilyDesc[] familys;
    private HFamilyDesc defaultFamily;
    private KeyPattern rowKeyPattern;
    private int presplitCount; // 预分区大小

    private Map<String, byte[]> columnFamilyMap;
    private byte[] defaultFamilyName;

    public HBaseSink(HBaseConnection connection, HBaseAdmin admin, int putThreadCount, String tableName, int presplitCount, HFamilyDesc[] familys, HFamilyDesc defaultFamily, KeyPattern rowKeyPattern) {
        Preconditions.checkNotNull(tableName, "tableName can not be null.");
        Preconditions.checkArgument(familys != null || defaultFamily != null, "familys or defaultFamily can not be null.");
        Preconditions.checkNotNull(rowKeyPattern, "rowKeyPattern can not be null.");
        this.putThreadCount = Math.max(putThreadCount, 1);
        this.connection = connection;
        this.admin = admin;
        this.tableName = tableName;
        this.presplitCount = presplitCount;
        this.familys = familys;
        this.defaultFamily = defaultFamily;
        this.rowKeyPattern = rowKeyPattern;
    }

    private void prestart() {
        // 生成字段与列簇对应表
        columnFamilyMap = new HashMap<>();
        if (familys != null) {
            for (HFamilyDesc family : familys) {
                byte[] familyName = Bytes.toBytes(family.getFamilyName());
                for (String column : family.getColumns()) {
                    columnFamilyMap.put(column.toLowerCase(), familyName);
                }
            }
        }
        // 默认列簇数据
        defaultFamilyName = defaultFamily != null ? Bytes.toBytes(defaultFamily.getFamilyName()) : null;
    }

    @Override
    public void start() {
        Preconditions.checkNotNull(channel, "Channel can not be null.");
        // 初始化服务
        prestart();
        // 启动数据写入线程
        new Thread(this).start();
        super.start();
        // 启动提交线程
        for (int i = 0; i < putThreadCount; i++) {
            new PutThread().start();
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public HFamilyDesc[] getFamilys() {
        return familys;
    }

    public void setFamilys(HFamilyDesc[] familys) {
        this.familys = familys;
    }

    public HFamilyDesc getDefaultFamily() {
        return defaultFamily;
    }

    public void setDefaultFamily(HFamilyDesc defaultFamily) {
        this.defaultFamily = defaultFamily;
    }

    public KeyPattern getRowKeyPattern() {
        return rowKeyPattern;
    }

    public void setRowKeyPattern(KeyPattern rowKeyPattern) {
        this.rowKeyPattern = rowKeyPattern;
    }

    @Override
    public void run() {
        int count = 0;
        long startTime = System.currentTimeMillis();

        // 取前BULK_SIZE长度数据，预分区HBase表
        boolean rowkeySpliting = true;
        List<String> preRowKey = new ArrayList<>(BULK_SIZE);
        Event event;
        List<Put> puts = new ArrayList<>(BULK_SIZE);
        do {
            event = channel.take();
            if (event != null) {
                Put put = newPut(event);
                if (rowkeySpliting) {
                    preRowKey.add(Bytes.toString(put.getRow()));
                }
                if (put != null) {
                    puts.add(put);
                    count++;
                }
            }
            // 批量提交
            if (puts.size() >= BULK_SIZE) {
                if (rowkeySpliting) {
                    rowkeySpliting = false;
                    // 计算分区Key和创建表结构
                    createTable(preRowKey, presplitCount);
                    preRowKey.clear();
                }
                while (putsQueue.size() > 3) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                    }
                }
                putsQueue.add(puts);
                puts = new ArrayList<>(BULK_SIZE);
            }
        } while (event != null && state == LifecycleState.START);
        // 提交最后的数据
        if (puts.size() > 0) {
            putsQueue.add(puts);
        }
        stop();
        logger.info("HBase sink ({}) imported {} objects; speed: {}ops.", tableName, count,
                count / Math.max((System.currentTimeMillis() - startTime) / 1000, 1));
    }

    private boolean createTable(List<String> preRowKey, int regionNum) {
        byte[][] splitKeys = null;
        // 排序RowKey
        List<String> rowKeys = Ordering.natural().sortedCopy(preRowKey);
        // 计算合适的分区Key
        if (rowKeys.size() < regionNum) {
            splitKeys = new byte[][]{Bytes.toBytes(rowKeys.get(0)), Bytes.toBytes(rowKeys.get(rowKeys.size() - 1))};
        } else {
            double step = rowKeys.size() / regionNum;
            splitKeys = new byte[regionNum - 1][];
            for (int j = 1; j < regionNum; j++) {
                splitKeys[j - 1] = Bytes.toBytes(preRowKey.get((int) (j * step)));
            }
        }

        // 检查和创建表结构
        try {
            admin.createTable(tableName, familys, splitKeys);
        } catch (TableExistsException e) {
        } catch (IOException e) {
            logger.error(String.format("Create hbase table(%s) failure", tableName), e);
            return false;
        }
        return true;
    }

    private Put newPut(Event event) {
        Put put = new Put(Bytes.toBytes(rowKeyPattern.format(event)));
        for (String key : event.getKeys()) {
            byte[] familyName = columnFamilyMap.get(key);
            familyName = familyName != null ? familyName : defaultFamilyName;
            if (familyName != null) {
                try {
                    Object v = event.get(key);
                    if (v != null) {
                        put.addColumn(familyName, Bytes.toBytes(key), toBytes(v.toString()));
                    }
                } catch (SQLException e) {
                    logger.error(String.format("Write hbase(Table name: %s) failure", tableName), e);
                }
            }
        }
        return put;
    }

    // 数据库Value转byte数组
    private byte[] toBytes(Object obj) throws SQLException {
        return Bytes.toBytes((String) obj);
        /*if (obj instanceof String) {
            return Bytes.toBytes((String) obj);
        } else if (obj instanceof BigDecimal) {
            return Bytes.toBytes((BigDecimal) obj);
        } else if (obj instanceof Boolean) {
            return Bytes.toBytes((Boolean) obj);
        } else if (obj instanceof Byte) {
            return Bytes.toBytes((Byte) obj);
        } else if (obj instanceof Long) {
            return Bytes.toBytes((Long) obj);
        } else if (obj instanceof Integer) {
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof Short) {
            return Bytes.toBytes((Short) obj);
        } else if (obj instanceof Double) {
            return Bytes.toBytes((Double) obj);
        } else if (obj instanceof Float) {
            return Bytes.toBytes((Float) obj);
        } else if (obj instanceof byte[]) {
            return (byte[]) obj;
        } else if (obj instanceof Date) {
            return Bytes.toBytes(((Date) obj).getTime());
        } else if (obj instanceof Blob) {
            // TODO Test
            Blob obj1 = (Blob) obj;
            return obj1.getBytes(0, (int) obj1.length());
        } else if (obj instanceof Clob) {
            // TODO Test
            Clob clob = (Clob) obj;
            return Bytes.toBytes(clob.getSubString(0, 0));
        } else {
            return Bytes.toBytes(obj.toString());
        }*/
    }

    // 提交数据线程
    class PutThread extends Thread {
        @Override
        public void run() {
            try (Table table = connection.getTable(tableName)) {
                while (true) {
                    List<Put> puts = putsQueue.poll();
                    if (null == puts) {
                        if (state != LifecycleState.START) {
                            break;
                        } else {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        table.put(puts);
                    }
                }
            } catch (IOException e) {
                logger.error(String.format("Write hbase(Table name: %s) failure", tableName), e);
            }
        }
    }
}

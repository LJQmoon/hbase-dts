/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 下午6:18
 */

package com.gsww.healthplatform.hbasedts.jdbc.source;

import com.google.common.base.Preconditions;
import com.gsww.healthplatform.hbasedts.arch.Channel;
import com.gsww.healthplatform.hbasedts.arch.Event;
import com.gsww.healthplatform.hbasedts.arch.Source;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.AbstractLifecycle;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleState;
import com.gsww.healthplatform.hbasedts.utils.InnerFunction;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcSource extends AbstractLifecycle implements Source, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);
    private String sql;
    private List<String> args;
    private List<Pair<String, String>> columns;
    private Channel channel;

    private JdbcTemplate jdbcTemplate;

    public JdbcSource(JdbcTemplate jdbcTemplate, String sql, List<String> args) {
        this.jdbcTemplate = jdbcTemplate;
        this.sql = sql;
        this.args = args;
    }

    @Override
    public void start() {
        Preconditions.checkNotNull(channel, "Channel can not be null.");
        new Thread(this).start();
        super.start();
    }

    @Override
    public void run() {
        List<Object> params = new ArrayList<>(args.size());
        try {
            for (String arg : args) {
                if (InnerFunction.isFunction(arg)) {
                    params.add(InnerFunction.execute(arg.substring(1, arg.length() - 1)));
                }
            }
            jdbcTemplate.query(sql, params.toArray(), new ResultSetExtractor<Object>() {
                @Override
                public Object extractData(ResultSet resultSet) throws SQLException, DataAccessException {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    List<Pair<String, String>> columns = new ArrayList<>(metaData.getColumnCount());
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i);
                        columns.add(new Pair<>(columnName, filterColumnName(columnName)));
                    }
                    JdbcSource.this.columns = columns;

                    while (resultSet.next()) {
                        Map<String, Object> row = readRow(resultSet);
                        channel.put(new Event(row));
                        if (channel.getState() != LifecycleState.START)
                            break;
                    }
                    stop();
                    return null;
                }
            });
        } catch (Exception e) {
            logger.error(String.format("Query sql(%s) failure", sql), e);
            stop();
            return;
        }
    }

    // Hive字段名默认为“表名.字段名”，使用此方法过滤其中的表名
    private final static String filterColumnName(String columnName) {
        int index = columnName.lastIndexOf('.');
        if (index >= 0) {
            return columnName.substring(index+1).toLowerCase();
        }
        return columnName.toLowerCase();
    }

    private Map<String, Object> readRow(ResultSet resultSet) throws SQLException {
        Map<String, Object> result = new HashMap<>(columns.size());
        for (Pair<String, String> column : columns) {
            Object value = resultSet.getObject(column.getKey());
            if (value != null) {
                result.put(column.getValue(), value);
            }
        }
        return result;
    }

    @Override
    public void stop() {
        if (channel != null) {
            channel.stop();
        }
        super.stop();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    @Override
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

}

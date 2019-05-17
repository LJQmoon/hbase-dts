/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.job;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonReader;
import com.gsww.healthplatform.hbasedts.arch.Job;
import com.gsww.healthplatform.hbasedts.arch.Sink;
import com.gsww.healthplatform.hbasedts.arch.Source;
import com.gsww.healthplatform.hbasedts.hbase.sink.HBaseSinkLoader;
import com.gsww.healthplatform.hbasedts.jdbc.source.JdbcSource;
import com.gsww.healthplatform.hbasedts.hbase.sink.HBaseSink;
import com.gsww.healthplatform.hbasedts.jdbc.source.JdbcSourceLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 任务加载器
 */
@Component
public class JobLoader {
    @Autowired
    private HBaseSinkLoader hBaseSinkLoader;

    @Autowired
    private JdbcSourceLoader jdbcSourceLoader;

    public List<Job> load(String confDir) throws IOException {
        List<Job> result = new ArrayList<>();
        File[] files = new File(confDir).listFiles((dir, name) -> name.endsWith(".dts"));
        if (files != null) {
            for (File file : files) {
                Job job = loadJobFile(file);
                result.add(job);
            }
        }
        return result;
    }

    private Job loadJobFile(File file) throws IOException {
        Source source = null;
        Sink sink = null;

        try (JsonReader reader = new JsonReader(new FileReader(file))) {
            reader.beginObject();
            while (reader.hasNext()) {
                String key = reader.nextName();
                if (key.equals("source")) {
                    source = readSource(reader);
                } else if (key.equals("sink")) {
                    sink = readSink(reader);
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
        }
        Preconditions.checkNotNull(source, "source can not be null.");
        Preconditions.checkNotNull(sink, "sink can not be null.");
        return new BasicJob(file.getAbsolutePath(), source, sink);
    }

    private Source readSource(JsonReader reader) throws IOException {
        return jdbcSourceLoader.load(reader);
    }

    private Sink readSink(JsonReader reader) throws IOException {
        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("hbase")) {
                return hBaseSinkLoader.load(reader);
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();
        return null;
    }
}

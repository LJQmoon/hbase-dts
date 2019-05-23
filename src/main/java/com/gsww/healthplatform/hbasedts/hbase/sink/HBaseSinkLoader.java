/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 下午6:56
 */

package com.gsww.healthplatform.hbasedts.hbase.sink;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.gsww.healthplatform.hbasedts.arch.base.KeyPattern;
import com.gsww.healthplatform.hbasedts.hbase.HBaseAdmin;
import com.gsww.healthplatform.hbasedts.hbase.HBaseConnection;
import com.gsww.healthplatform.hbasedts.hbase.write.HFamilyDesc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class HBaseSinkLoader {
    public static final String JSON_NAME = "hbase";

    @Autowired
    private HBaseConnection hBaseConnection;
    @Autowired
    private HBaseAdmin hBaseAdmin;

    @Value("${hbase.put_threads}")
    private int putThreads;


    public final HBaseSink load(JsonReader reader) throws IOException {
        String tableName = null;
        List<HFamilyDesc> familys = new ArrayList<>();
        HFamilyDesc defaultFamily = null;
        KeyPattern rowKeyPattern = null;
        int presplitCount = 3;  // 预分区数量

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("table_name") && reader.peek() != JsonToken.NULL) {
                tableName = reader.nextString();
            } else if (key.equals("presplit_count") && reader.peek() == JsonToken.NUMBER) {
                presplitCount = reader.nextInt();
            } else if (key.equals("familys") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    HFamilyDesc family = loadFamily(reader);
                    if (family != null) {
                        familys.add(family);
                    }
                }
                reader.endArray();
            } else if (key.equals("default_family") && reader.peek() == JsonToken.BEGIN_OBJECT) {
                defaultFamily = loadFamily(reader);
            } else if (key.equals("row_key") && reader.peek() == JsonToken.BEGIN_OBJECT) {
                rowKeyPattern = KeyPattern.load(reader);
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        Preconditions.checkNotNull(tableName, "table_name can not be null.");
        Preconditions.checkArgument(familys != null || defaultFamily != null, "familys or default_family can not be null.");
        Preconditions.checkNotNull(rowKeyPattern, "row_key can not be null.");
        return new HBaseSink(hBaseConnection, hBaseAdmin, putThreads, tableName, presplitCount,
                (HFamilyDesc[]) familys.toArray(new HFamilyDesc[]{}), defaultFamily, rowKeyPattern);
    }

    private static final HFamilyDesc loadFamily(JsonReader reader) throws IOException {
        String familyName = null;
        Integer blocksize = null;
        List<String> columns = new ArrayList<>();

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("family_name") && reader.peek() != JsonToken.NULL) {
                familyName = reader.nextString();
            } else if (key.equals("block_size") && reader.peek() != JsonToken.NULL) {
                blocksize = reader.nextInt();
            } else if (key.equals("columns") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    columns.add(reader.nextString());
                }
                reader.endArray();
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        if (familyName != null) {
            HFamilyDesc result = new HFamilyDesc(familyName);
            if (blocksize != null) {
                result.setBlockSize(blocksize);
            }
            if (columns.size() > 0) {
                String[] arr = new String[columns.size()];
                result.addColumn(columns.toArray(arr));
            }
            return result;
        }
        return null;
    }
}

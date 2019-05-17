/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-4 下午5:54
 */

package com.gsww.healthplatform.hbasedts.jdbc.source;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.gsww.healthplatform.hbasedts.arch.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
public class JdbcSourceLoader {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Source load(JsonReader reader) throws IOException {
        String sql = null;
        List<String> args = new ArrayList<>();

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("sql") && reader.peek() != JsonToken.NULL) {
                sql = reader.nextString();
            } else if (key.equals("args") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    args.add(reader.nextString());
                }
                reader.endArray();
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        if (sql != null)
            return new JdbcSource(jdbcTemplate, sql, args);
        return null;
    }
}

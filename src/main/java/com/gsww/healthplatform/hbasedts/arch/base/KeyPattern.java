/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-7 上午11:10
 */

package com.gsww.healthplatform.hbasedts.arch.base;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.gsww.healthplatform.hbasedts.arch.Event;
import com.gsww.healthplatform.hbasedts.utils.InnerFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyPattern {
    private String pattern;
    private List<String> args;

    public KeyPattern(String pattern, List<String> args) {
        this.pattern = pattern;
        // 统一转为小写
        List<String> tmp = new ArrayList<>(args.size());
        for (String arg : args) {
            tmp.add(arg.toLowerCase());
        }
        this.args = tmp;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public List<String> getArgs() {
        return args;
    }

    public void setArgs(List<String> args) {
        this.args = args;
    }

    public String format(Event event) {
        String format = pattern.replaceAll("\\?", "%s");
        List<String> params = new ArrayList<>();
        for (String arg : args) {
            if (InnerFunction.isFunction(arg)) {
                params.add(InnerFunction.execute(arg.substring(1, arg.length()-1)).toString());
            } else {
                Object value = event.get(arg);
                params.add(value != null ? value.toString() : "");
            }
        }
        return String.format(format, params.toArray());
    }

    public static final KeyPattern load(JsonReader reader) throws IOException {
        String pattern = null;
        List<String> args = new ArrayList<>();
        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("pattern") && reader.peek() != JsonToken.NULL) {
                pattern = reader.nextString();
            } else if (key.equals("args") && reader.peek() == JsonToken.BEGIN_ARRAY) {
                reader.beginArray();
                while (reader.hasNext()) {
                    args.add(reader.nextString());
                }
                reader.endArray();
            }
        }
        reader.endObject();

        if (pattern != null) {
            return new KeyPattern(pattern, args);
        }
        return null;
    }

    public static void main(String[] args) {
        List<String> params = new ArrayList<>();
        params.add("{date}");
        params.add("id");
        KeyPattern pattern = new KeyPattern("id_?_date_?", params);
        Map<String, Object> row = new HashMap<>();
        row.put("id", 18741786348734L);
        System.out.println(pattern.format(new Event(row)));
    }
}

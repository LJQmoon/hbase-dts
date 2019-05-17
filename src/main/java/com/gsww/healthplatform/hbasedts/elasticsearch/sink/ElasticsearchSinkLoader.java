/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-6 下午5:05
 */

package com.gsww.healthplatform.hbasedts.elasticsearch.sink;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.gsww.healthplatform.hbasedts.arch.base.KeyPattern;
import com.gsww.healthplatform.hbasedts.arch.Sink;
import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticsearchSinkLoader {
    public static final String JSON_NAME = "elasticsearch";

    public static final Sink load(JsonReader reader) throws IOException {
        List<HttpHost> hosts = null;
        String index = null;
        String type = null;
        KeyPattern idPattern = null;

        reader.beginObject();
        while (reader.hasNext()) {
            String key = reader.nextName();
            if (key.equals("hosts")) {
                hosts = readHosts(reader);
            } else if (key.equals("index") && reader.peek() == JsonToken.STRING) {
                index = reader.nextString();
            } else if (key.equals("type") && reader.peek() == JsonToken.STRING) {
                type = reader.nextString();
            } else if (key.equals("id") && reader.peek() == JsonToken.BEGIN_OBJECT) {
                idPattern = KeyPattern.load(reader);
            } else {
                reader.skipValue();
            }
        }
        reader.endObject();

        Preconditions.checkNotNull(hosts, "sink.elasticsearch.hosts can not be null.");
        Preconditions.checkNotNull(index, "sink.elasticsearch.index can not be null.");
        Preconditions.checkNotNull(type, "sink.elasticsearch.type can not be null.");
        if (hosts != null && index != null && type != null) {
            return new ElasticsearchSink(hosts.toArray(new HttpHost[]{}), index, type, idPattern);
        }
        return null;
    }

    private static List<HttpHost> readHosts(JsonReader reader) throws IOException {
        List<HttpHost> hosts = new ArrayList<>();
        reader.beginArray();
        while (reader.hasNext()) {
            String hostname = null;
            Integer port = null;

            reader.beginObject();
            while (reader.hasNext()) {
                String key = reader.nextName();
                if (key.equals("hostname") && reader.peek() == JsonToken.STRING) {
                    hostname = reader.nextString();
                } else if (key.equals("port") && reader.peek() == JsonToken.NUMBER) {
                    port = reader.nextInt();
                } else {
                    reader.skipValue();
                }
            }
            reader.endObject();
            if (hostname != null && port != null) {
                hosts.add(new HttpHost(hostname, port));
            }
        }
        reader.endArray();
        return hosts.size() > 0 ? hosts : null;
    }
}

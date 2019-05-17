/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 下午6:32
 */

package com.gsww.healthplatform.hbasedts.arch;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Event {
    private Map<String, Object> data;

    public Event() {
        data = new HashMap<>();
    }

    public Event(Map<String, Object> data) {
        this.data = data;
    }

    public Object get(String key) {
        if (data != null) {
            return data.get(key);
        }
        return null;
    }

    public Set<String> getKeys() {
        return data.keySet();
    }
}

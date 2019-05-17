/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-9 下午2:48
 */

package com.gsww.healthplatform.hbasedts.utils;

import java.util.concurrent.atomic.AtomicInteger;

public class RefCount<T> {
    private AtomicInteger refCount = new AtomicInteger(1);
    private final T obj;

    public RefCount(T obj) {
        this.obj = obj;
    }

    public T get() {
        return obj;
    }

    public int getRefCount() {
        return refCount.get();
    }

    public int addRef() {
        int andIncrement = refCount.incrementAndGet();
        return andIncrement;
    }

    public int release() {
        int decrementAndGet = refCount.decrementAndGet();
        return decrementAndGet;
    }
}

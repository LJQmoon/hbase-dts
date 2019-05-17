/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-4 下午12:13
 */

package com.gsww.healthplatform.hbasedts.arch.lifecycle;

public abstract class AbstractLifecycle implements Lifecycle {
    protected LifecycleState state = LifecycleState.IDLE;

    @Override
    public void start() {
        state = LifecycleState.START;
    }

    @Override
    public void stop() {
        state = LifecycleState.STOP;
    }

    @Override
    public LifecycleState getState() {
        return state;
    }
}

/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 下午5:12
 */

package com.gsww.healthplatform.hbasedts.arch.lifecycle;

public interface Lifecycle {
    void start();
    void stop();
    LifecycleState getState();
}

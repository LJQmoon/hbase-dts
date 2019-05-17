/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-4 下午12:52
 */

package com.gsww.healthplatform.hbasedts.arch.lifecycle;

public class LifecycleUtils {
    public static final int INTERVAL_TIME = 500;

    public static void waitFor(Lifecycle lifecycle) {
        while (lifecycle.getState() == LifecycleState.START) {
            try {
                Thread.sleep(INTERVAL_TIME);
            } catch (InterruptedException e) {
            }
        }
    }
}

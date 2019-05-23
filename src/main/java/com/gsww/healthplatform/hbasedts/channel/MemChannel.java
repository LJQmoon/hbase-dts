/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 下午5:23
 */

package com.gsww.healthplatform.hbasedts.channel;

import com.gsww.healthplatform.hbasedts.arch.Channel;
import com.gsww.healthplatform.hbasedts.arch.Event;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.AbstractLifecycle;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleState;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemChannel extends AbstractLifecycle implements Channel {
    private Queue<Event> eventQueue;
    private int bufferSize = 50000;

    public MemChannel() {
        state = LifecycleState.IDLE;
        eventQueue = new ConcurrentLinkedQueue<>();
    }

    public MemChannel(int bufferSize) {
        super();
        this.bufferSize = bufferSize;
    }

    @Override
    public void put(Event event) {
        while ((eventQueue.size() > bufferSize)) {
            if (state != LifecycleState.START)
                break;
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
        }
        eventQueue.add(event);
    }

    @Override
    public Event take() {
        Event e;
        while (true) {
            e = eventQueue.poll();
            // 获取到数据或非运行状态退出循环
            if (e != null || LifecycleState.START != state) {
                break;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e1) {
            }
        }
        return e;
    }

    @Override
    public void stop() {
        super.stop();
    }
}

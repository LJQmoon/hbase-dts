/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 下午6:17
 */

package com.gsww.healthplatform.hbasedts.job;

import com.google.common.base.Preconditions;
import com.gsww.healthplatform.hbasedts.arch.Channel;
import com.gsww.healthplatform.hbasedts.arch.Job;
import com.gsww.healthplatform.hbasedts.arch.Sink;
import com.gsww.healthplatform.hbasedts.arch.Source;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.AbstractLifecycle;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleState;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleUtils;
import com.gsww.healthplatform.hbasedts.channel.MemChannel;

public class BasicJob extends AbstractLifecycle implements Job {
    private String id;
    private Source source;
    private Sink sink;
    private Channel channel;

    public BasicJob(String id, Source source, Sink sink) {
        this.id = id;
        this.source = source;
        this.sink = sink;
        this.channel = new MemChannel();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void execute() {
        Preconditions.checkNotNull(source, "Souce can not be null.");
        Preconditions.checkNotNull(sink, "Sink can not be null.");
        source.setChannel(channel);
        sink.setChannel(channel);
        // 启动
        channel.start();
        source.start();
        sink.start();
        // 等待结束
        LifecycleUtils.waitFor(source);
        LifecycleUtils.waitFor(sink);
        // 结束，清理
        channel.stop();
        super.stop();
    }
}

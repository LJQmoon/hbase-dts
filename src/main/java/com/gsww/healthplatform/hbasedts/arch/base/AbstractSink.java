/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-7 上午11:11
 */

package com.gsww.healthplatform.hbasedts.arch.base;

import com.gsww.healthplatform.hbasedts.arch.Channel;
import com.gsww.healthplatform.hbasedts.arch.Sink;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.AbstractLifecycle;

public abstract class AbstractSink extends AbstractLifecycle implements Sink {
    protected Channel channel;

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    @Override
    public void stop() {
        if (channel != null) {
            channel.stop();
        }
        super.stop();
    }
}

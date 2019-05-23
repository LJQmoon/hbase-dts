/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-4 下午5:18
 */

package com.gsww.healthplatform.hbasedts;

import com.gsww.healthplatform.hbasedts.job.JobManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class AppMain implements CommandLineRunner {
    @Autowired
    private JobManager jobManager;

    @Override
    public void run(String... args) throws Exception {
        long startTime = System.currentTimeMillis();
        jobManager.start();
        System.out.println(System.currentTimeMillis() - startTime);
        System.exit(0);
    }
}

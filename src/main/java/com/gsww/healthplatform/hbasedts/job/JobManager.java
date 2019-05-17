/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.job;

import com.gsww.healthplatform.hbasedts.arch.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Component
public class JobManager {
    private static final Logger logger = LoggerFactory.getLogger(JobManager.class);

    @Value("${dts.conf_dir}")
    private String confDir;

    @Value("${dts.concurrent_num}")
    private int concurrentNum;

    @Autowired
    private JobLoader jobLoader;

    private List<Job> jobs;

    @PostConstruct
    private void init() {
        try {
            jobs = jobLoader.load(confDir);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void start() {
        if (null == jobs)
            return;
        // TODO 控制任务同时执行数量
        for (Job job : jobs) {
            job.execute();
        }
    }
}

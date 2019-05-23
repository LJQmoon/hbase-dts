/*
 * Copyright 中国电信甘肃万维公司 All rights reserved.
 * 中国电信甘肃万维公司 专有/保密源代码,未经许可禁止任何人通过任何* 渠道使用、修改源代码.
 *
 * Author: yangpy
 * Created: 18-11-3 上午11:55
 */

package com.gsww.healthplatform.hbasedts.job;

import com.gsww.healthplatform.hbasedts.arch.Job;
import com.gsww.healthplatform.hbasedts.arch.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
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
        List<Job> activeJob = new ArrayList<>(jobs.size());
        int runningJobs;
        do {
            // 统计运行任务数量和活跃任务
            runningJobs = 0;
            activeJob.clear();
            for (Job job : jobs) {
                switch (job.getState()) {
                    case IDLE:
                        activeJob.add(job);
                        break;
                    case START:
                        activeJob.add(job);
                        runningJobs++;
                        break;
                }
            }

            // 如果运行任务小于并发任务数据，则启动排队任务
            if (runningJobs < concurrentNum) {
                for (int i = 0; i < concurrentNum - runningJobs; i++) {
                    for (Job job : activeJob) {
                        if (job.getState() == LifecycleState.IDLE) {
                            job.start();
                            break;
                        }
                    }
                }
            }
            // 每个处理间隔3秒
            if (activeJob.size() > 0) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }
            }
        } while (activeJob.size() > 0);
    }
}

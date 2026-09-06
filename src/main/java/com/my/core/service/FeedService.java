package com.my.core.service;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.stereotype.Service;

@Service
public class FeedService {

    private final JobOperator jobOperator;
    private final Job batchJavaJob;
    private final Job batchDuckdbJob;
    private final Job batchJavaPartitionedJob;

    public FeedService(JobOperator jobOperator, Job batchJavaJob, Job batchDuckdbJob, Job batchJavaPartitionedJob) {
        this.jobOperator = jobOperator;
        this.batchJavaJob = batchJavaJob;
        this.batchDuckdbJob = batchDuckdbJob;
        this.batchJavaPartitionedJob = batchJavaPartitionedJob;
    }

    public JobExecution startJavaProcess() throws Exception {
        var jobParameters = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        return jobOperator.start(batchJavaJob, jobParameters);
    }

    public JobExecution startDuckDbProcess() throws Exception {
        var jobParameters = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        return jobOperator.start(batchDuckdbJob, jobParameters);
    }

    public JobExecution startPartitionedJavaProcess() throws Exception {
        var jobParameters = new JobParametersBuilder()
                .addLong("timestamp", System.currentTimeMillis())
                .toJobParameters();

        return jobOperator.start(batchJavaPartitionedJob, jobParameters);
    }
}

package com.my.core.config;

import com.my.core.tasklet.DuckDbTransformTasklet;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
class BatchDuckDbConfig {

    @Value("${app.duckdb.url}")
    private String duckdbUrl;

    @Bean
    Step duckDbTransformStep(JobRepository jobRepository,
                             Resource feedResource,
                             PlatformTransactionManager transactionManager) {
        return new StepBuilder("duckDbTransform", jobRepository)
                .tasklet(new DuckDbTransformTasklet(duckdbUrl, feedResource), transactionManager)
                .build();
    }

    @Bean
    Job batchDuckdbJob(JobRepository jobRepository,
                       Step duckDbTransformStep) {
        return new JobBuilder("batchDuckdbJob", jobRepository)
                .start(duckDbTransformStep)
                .build();
    }
}
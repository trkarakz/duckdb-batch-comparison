package com.my.core.config;

import com.my.core.csv.*;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepSynchronizationManager;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import tools.jackson.dataformat.csv.CsvMapper;

import java.nio.file.Path;
import java.util.Map;

@Configuration
class BatchConfig {

    @Value("${app.partitioned.chunk-size:10_000}")
    private int chunkSize;

    @Value("${app.feed:feed.csv}")
    private String feed;

    @Bean
    public Resource feedResource() {
        return new ClassPathResource(feed);
    }

    @Bean
    protected CsvMapper csvMapper() {
        return new CsvMapper();
    }

    @Bean
    public FeedRowToCsvExtractor feedExtractor() {
        return new FeedRowToCsvExtractor();
    }

    @Bean
    @JobScope
    protected Step processFile(CsvReader csvReader,
                               CsvToFeedRowProcessor csvToFeedRowProcessor,
                               ItemWriter<FeedRow> outputFileWriter,
                               JobRepository jobRepository) {
        return new StepBuilder("processFeed", jobRepository)
                .<Map<String, String>, FeedRow>chunk(chunkSize)
                .faultTolerant()
                .stream(csvReader)
                .reader(csvReader)
                .processor(item -> {
                    long t0 = System.currentTimeMillis();
                    try {
                        return csvToFeedRowProcessor.mapLine(item);
                    } finally {
                        var context = StepSynchronizationManager.getContext();
                        if (context != null) {
                            StepExecution se = context.getStepExecution();
                            long acc = se.getExecutionContext().getLong("processorMs", 0L);
                            se.getExecutionContext().putLong("processorMs", acc + (System.currentTimeMillis() - t0));
                        }
                    }
                })
                .writer(outputFileWriter)
                .listener(stepTimingListener())
                .build();
    }

    @Bean(name = "outputFileWriter")
    @JobScope
    protected FlatFileItemWriter<FeedRow> outputFileWriter(CsvMapper csvMapper, FeedRowToCsvExtractor feedExtractor) {
        var lineAggregator = new CsvLineAggregator<FeedRow>(csvMapper);
        lineAggregator.setFieldExtractor(feedExtractor);

        var writer = new FlatFileItemWriter<>(lineAggregator);
        writer.setResource(new FileSystemResource(Path.of("./output.csv")));
        writer.setAppendAllowed(false);
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(lineAggregator);
        writer.setHeaderCallback(w -> w.write("model,price"));

        return writer;
    }

    @Bean
    Job batchJavaJob(JobRepository jobRepository, Step processFile) {
        return new JobBuilder("batchJavaJob", jobRepository)
                .start(processFile)
                .build();
    }

    @Bean
    public StepExecutionListener stepTimingListener() {
        return new StepExecutionListener() {
            private final Logger log = LoggerFactory.getLogger("StepTiming");

            @Override
            public void beforeStep(@NonNull StepExecution stepExecution) {
                stepExecution.getExecutionContext().putLong("startNanos", System.nanoTime());
            }

            @Override
            public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
                long start = stepExecution.getExecutionContext().getLong("startNanos", -1L);

                if (start > 0) {
                    long processorMs = stepExecution.getExecutionContext().getLong("processorMs", 0L);
                    double totalMs = (System.nanoTime() - start) / 1_000_000.0;

                    log.info(
                            "Step '{}' finished in {} ms (processor={} ms, read={}, write={}, skip={})",
                            stepExecution.getStepName(),
                            java.math.BigDecimal.valueOf(totalMs).setScale(3, java.math.RoundingMode.HALF_UP).toPlainString(),
                            processorMs,
                            stepExecution.getReadCount(),
                            stepExecution.getWriteCount(),
                            stepExecution.getSkipCount()
                    );
                }

                return stepExecution.getExitStatus();
            }
        };
    }
}
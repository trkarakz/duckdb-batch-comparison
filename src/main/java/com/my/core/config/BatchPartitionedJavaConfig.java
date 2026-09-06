package com.my.core.config;

import com.my.core.csv.*;
import com.my.core.partitioner.FilePartitioner;
import com.my.core.tasklet.MergePartitionOutputsTasklet;
import com.my.core.tasklet.SplitFeedTasklet;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.dataformat.csv.CsvMapper;

import java.nio.file.Path;
import java.util.Map;

@Configuration
public class BatchPartitionedJavaConfig {

    @Value("${app.partitioned.work-dir:./batch-partitioned}")
    private String workDir;

    @Value("${app.partitioned.grid-size:20}")
    private int gridSize;

    @Value("${app.partitioned.chunk-size:10000}")
    private int chunkSize;

    @Bean
    TaskExecutor partitionTaskExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(gridSize);
        executor.setMaxPoolSize(gridSize);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("partition-worker-");
        executor.initialize();
        return executor;
    }

    @Bean
    Step splitFeedStep(JobRepository jobRepository,
                       PlatformTransactionManager transactionManager,
                       Resource feedResource) {
        return new StepBuilder("splitFeedStep", jobRepository)
                .tasklet(new SplitFeedTasklet(feedResource, Path.of(workDir), gridSize), transactionManager)
                .build();
    }

    @Bean
    Partitioner filePartitioner() {
        return new FilePartitioner(Path.of(workDir, "input"), Path.of(workDir, "output"));
    }

    @Bean
    Step partitionWorkerStep(JobRepository jobRepository,
                             PartitionCsvReader partitionCsvReader,
                             CsvToFeedRowProcessor csvToFeedRowProcessor,
                             FlatFileItemWriter<FeedRow> partitionOutputWriter) {
        return new StepBuilder("partitionWorkerStep", jobRepository)
                .<Map<String, String>, FeedRow>chunk(chunkSize)
                .reader(partitionCsvReader)
                .processor(csvToFeedRowProcessor::mapLine)
                .writer(partitionOutputWriter)
                .build();
    }

    @Bean
    Step partitionedMasterStep(JobRepository jobRepository,
                               Partitioner filePartitioner,
                               Step partitionWorkerStep,
                               TaskExecutor partitionTaskExecutor) {
        return new StepBuilder("partitionedMasterStep", jobRepository)
                .partitioner("partitionWorkerStep", filePartitioner)
                .step(partitionWorkerStep)
                .taskExecutor(partitionTaskExecutor)
                .gridSize(gridSize)
                .build();
    }

    @Bean
    Step mergePartitionOutputsStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager) {
        return new StepBuilder("mergePartitionOutputsStep", jobRepository)
                .tasklet(new MergePartitionOutputsTasklet(Path.of(workDir), Path.of("./output-partitioned.csv")), transactionManager)
                .build();
    }

    @Bean
    Job batchJavaPartitionedJob(JobRepository jobRepository,
                                Step splitFeedStep,
                                Step partitionedMasterStep,
                                Step mergePartitionOutputsStep) {
        return new JobBuilder("batchJavaPartitionedJob", jobRepository)
                .start(splitFeedStep)
                .next(partitionedMasterStep)
                .next(mergePartitionOutputsStep)
                .build();
    }

    @Bean
    @StepScope
    PartitionCsvReader partitionCsvReader(CsvMapper csvMapper,
                                          @Value("#{stepExecutionContext['inputFile']}") String inputFile) {
        return new PartitionCsvReader(csvMapper, Path.of(inputFile));
    }

    @Bean
    @StepScope
    FlatFileItemWriter<FeedRow> partitionOutputWriter(CsvMapper csvMapper,
                                                      FeedRowToCsvExtractor feedExtractor,
                                                      @Value("#{stepExecutionContext['outputFile']}") String outputFile) {
        var lineAggregator = new CsvLineAggregator<FeedRow>(csvMapper);
        lineAggregator.setFieldExtractor(feedExtractor);

        var writer = new FlatFileItemWriter<>(lineAggregator);
        writer.setResource(new org.springframework.core.io.FileSystemResource(Path.of(outputFile)));
        writer.setAppendAllowed(false);
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(lineAggregator);
        writer.setHeaderCallback(w -> w.write("model,price"));
        return writer;
    }
}

package com.my.core.config;

import com.my.core.csv.*;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
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
                .processor(csvToFeedRowProcessor::mapLine)
                .writer(outputFileWriter)
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
}
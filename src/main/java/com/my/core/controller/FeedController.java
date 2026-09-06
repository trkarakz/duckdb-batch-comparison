package com.my.core.controller;

import com.my.core.service.FeedService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Tag(name = "Feed")
@RestController
public class FeedController {
    private final FeedService feedService;

    public FeedController(FeedService feedService) {
        this.feedService = feedService;
    }

    @Operation(summary = "Start feed processing job")
    @PostMapping("/feeds/process")
    public ResponseEntity<Map<String, Object>> processBatch() throws Exception {
        return toResponse(feedService.startJavaProcess());
    }

    @Operation(summary = "Start DuckDB feed processing job")
    @PostMapping("/feeds/process-duckdb")
    public ResponseEntity<Map<String, Object>> processDuckDb() throws Exception {
        return toResponse(feedService.startDuckDbProcess());
    }

    @Operation(summary = "Start partitioned feed processing job")
    @PostMapping("/feeds/process-java-partitioned")
    public ResponseEntity<Map<String, Object>> processJavaPartitioned() throws Exception {
        return toResponse(feedService.startPartitionedJavaProcess());
    }

    private ResponseEntity<Map<String, Object>> toResponse(org.springframework.batch.core.job.JobExecution jobExecution) {
        var start = jobExecution.getStartTime();
        var end = jobExecution.getEndTime();

        Long durationMs = null;
        if (start != null && end != null) {
            durationMs = java.time.Duration.between(start, end).toMillis();
        }

        return ResponseEntity.ok(Map.of(
                "jobId", jobExecution.getJobInstanceId(),
                "executionId", jobExecution.getId(),
                "status", String.valueOf(jobExecution.getStatus()),
                "startTime", String.valueOf(start),
                "endTime", String.valueOf(end),
                "durationMs", durationMs == null ? "RUNNING" : durationMs
        ));
    }
}

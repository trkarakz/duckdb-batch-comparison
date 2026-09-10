# DuckDB + Spring Batch Comparison

A sample Spring Boot project that compares batch-processing approaches using:

- **Spring Batch Java processing**
- **DuckDB-powered SQL transformation**
- **Partitioned batch execution** (split/process/merge)

The project demonstrates reading CSV feed data, transforming it, and writing output CSV files using different batch strategies.

---

## Project Structure

```text
src/main/java/com/my/core/
  config/
    BatchConfig.java
    BatchDuckDbConfig.java
    BatchPartitionedJavaConfig.java
  controller/
    FeedController.java
  csv/
    CsvReader.java
    CsvToFeedRowProcessor.java
    CsvLineAggregator.java
    FeedRow.java
    FeedRowToCsvExtractor.java
    PartitionCsvReader.java
  partitioner/
    FilePartitioner.java
  service/
    FeedService.java
  tasklet/
    DuckDbTransformTasklet.java
    SplitFeedTasklet.java
    MergePartitionOutputsTasklet.java
```

---
## Tech Stack

```text
Java 25 (Spring Boot)
Spring Batch
DuckDB
Maven
```

## How It Works
This project includes multiple batch configurations:
- Standard Spring Batch flow
Reads input CSV, maps rows (FeedRow), processes/transforms, and writes CSV output.
- DuckDB-based flow
Uses DuckDbTransformTasklet to apply SQL-style transformations over CSV data and produce output.
- Partitioned flow
  - SplitFeedTasklet splits input into smaller CSV chunks.
  - FilePartitioner + partition step processes chunks in parallel/partitioned manner.
  - MergePartitionOutputsTasklet combines partition outputs into a final file.

## Build & Run

- `mvnw.cmd clean package`
- `mvnw.cmd spring-boot:run`

Then access swagger UI at: [http://localhost:8080/swagger-ui/index.html](http://localhost:8080/swagger-ui/index.html)

## Triggering Jobs / API
A controller exists at **controller/FeedController.java**, which indicates the app exposes HTTP endpoints to trigger or manage feed processing.
After starting the app, check available endpoints in that controller and call them using your preferred HTTP client (browser, curl, Postman, etc.).

## Input / Output
**Inputs**
 - src/main/resources/feed.csv
 - src/main/resources/feed2.csv
 - partitioned inputs under batch-partitioned/input/

**Outputs**
 - output.csv (standard batch output)
 - duckDbOutput.csv (DuckDB flow output)
 - output-partitioned.csv (merged partitioned output)
 - intermediate partition outputs under batch-partitioned/output/

## Configuration
Main app config is in:
 - src/main/resources/application.yaml

Batch job definitions are in:
 - config/BatchConfig.java
 - config/BatchDuckDbConfig.java
 - config/BatchPartitionedJavaConfig.java

## Problem Being Solved

Many teams process large CSV feeds with **Spring Batch** using Java-based readers/processors/writers.  
This works well, but as data volume grows, teams often ask:

- Can SQL-based engines like **DuckDB** perform the same transformations faster or more simply?
- When should we keep logic in Spring Batch processors vs move transformation logic into SQL?
- Does **partitioned processing** improve throughput enough to justify added complexity?

This project exists to compare these approaches on the same type of input/output workflow.

### Core Problem Statement

Given the same feed data and expected CSV output, determine the trade-offs between:

1. **Standard Spring Batch transformation**
2. **DuckDB SQL transformation**
3. **Partitioned Spring Batch processing**

The comparison focuses on:

- implementation complexity
- maintainability of transformation logic
- execution performance and scalability
- operational behavior (splitting, parallelism, merging outputs)

### Why This Matters

In real production pipelines, batch jobs must be:

- fast enough for SLA windows
- easy to modify when business rules change
- reliable when input volume spikes

Choosing the wrong approach can lead to slow jobs, hard-to-maintain code, or brittle operations.

### What “Solved” Means in This Repo

The project provides a practical baseline to evaluate:

- how each approach is configured and executed
- what output artifacts each flow produces
- how easy it is to reason about and evolve each pipeline

It is intended as a **comparison playground** for architectural decisions, not just a single implementation.
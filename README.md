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
package com.my.core.tasklet;

import org.jspecify.annotations.NonNull;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class SplitFeedTasklet implements Tasklet {

    private final Resource feedResource;
    private final Path workDir;
    private final int partitionCount;

    public SplitFeedTasklet(Resource feedResource, Path workDir, int partitionCount) {
        this.feedResource = feedResource;
        this.workDir = workDir;
        this.partitionCount = partitionCount;
    }

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution, @NonNull ChunkContext chunkContext) throws Exception {
        var inputDir = workDir.resolve("input");
        var outputDir = workDir.resolve("output");

        Files.createDirectories(inputDir);
        Files.createDirectories(outputDir);

        // Clean stale files from prior runs.
        deleteCsvFiles(inputDir);
        deleteCsvFiles(outputDir);

        var partWriters = new ArrayList<BufferedWriter>();
        try (BufferedReader reader = Files.newBufferedReader(feedResource.getFilePath(), StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                throw new IllegalStateException("feed.csv is empty");
            }

            for (int i = 0; i < partitionCount; i++) {
                var partFile = inputDir.resolve("part-" + i + ".csv");
                var w = Files.newBufferedWriter(partFile, StandardCharsets.UTF_8);
                w.write(header);
                w.newLine();
                partWriters.add(w);
            }

            String line;
            int row = 0;
            while ((line = reader.readLine()) != null) {
                int partition = row % partitionCount;
                var w = partWriters.get(partition);
                w.write(line);
                w.newLine();
                row++;
            }

            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext()
                    .putInt("partitionCount", partitionCount);
            chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext()
                    .putString("partitionWorkDir", workDir.toAbsolutePath().toString());
        } finally {
            for (var w : partWriters) {
                try { w.close(); } catch (Exception ignored) {}
            }
        }

        return RepeatStatus.FINISHED;
    }

    private static void deleteCsvFiles(Path dir) throws Exception {
        try (var stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".csv"))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof Exception e) {
                throw e;
            }
            throw ex;
        }
    }
}

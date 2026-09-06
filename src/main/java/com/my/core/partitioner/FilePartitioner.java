package com.my.core.partitioner;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullMarked;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FilePartitioner implements Partitioner {

    private final Path inputDir;
    private final Path outputDir;

    public FilePartitioner(Path inputDir, Path outputDir) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
    }

    @Override
    @NullMarked
    public Map<String, ExecutionContext> partition(int gridSize) {
        try (var filesStream = Files.list(inputDir)) {
            var files = filesStream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("part-"))
                    .filter(p -> p.getFileName().toString().endsWith(".csv"))
                    .sorted()
                    .collect(Collectors.toList());

            return createPartitions(files);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to build partitions from " + inputDir, e);
        }
    }

    private @NonNull LinkedHashMap<String, ExecutionContext> createPartitions(List<Path> files) {
        var partitions = new LinkedHashMap<String, ExecutionContext>();
        int i = 0;
        for (var inputFile : files) {
            var ctx = new ExecutionContext();
            var outputFile = outputDir.resolve(("out-" + i + ".csv")).toAbsolutePath().toString();

            ctx.putString("inputFile", inputFile.toAbsolutePath().toString());
            ctx.putString("outputFile", outputFile);
            ctx.putInt("partitionNumber", i);

            partitions.put("partition-" + i, ctx);
            i++;
        }
        return partitions;
    }
}

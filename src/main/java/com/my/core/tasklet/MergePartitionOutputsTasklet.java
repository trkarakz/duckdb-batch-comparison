package com.my.core.tasklet;

import org.jspecify.annotations.NonNull;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

public class MergePartitionOutputsTasklet implements Tasklet {

    private final Path workDir;
    private final Path finalOutputFile;

    public MergePartitionOutputsTasklet(Path workDir, Path finalOutputFile) {
        this.workDir = workDir;
        this.finalOutputFile = finalOutputFile;
    }

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution, @NonNull ChunkContext chunkContext) throws Exception {
        var outputDir = workDir.resolve("output");

        List<Path> files;
        try (var stream = Files.list(outputDir)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().startsWith("out-"))
                    .filter(p -> p.getFileName().toString().endsWith(".csv"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .toList();
        }

        Files.deleteIfExists(finalOutputFile);
        Files.createDirectories(finalOutputFile.toAbsolutePath().getParent());

        try (BufferedWriter out = Files.newBufferedWriter(finalOutputFile, StandardCharsets.UTF_8)) {
            boolean headerWritten = false;

            for (Path file : files) {
                try (BufferedReader in = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                    String line = in.readLine(); // header
                    if (line == null) {
                        continue;
                    }

                    if (!headerWritten) {
                        out.write(line);
                        out.newLine();
                        headerWritten = true;
                    }

                    while ((line = in.readLine()) != null) {
                        out.write(line);
                        out.newLine();
                    }
                }
            }
        }

        return RepeatStatus.FINISHED;
    }
}

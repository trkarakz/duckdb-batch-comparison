package com.my.core.csv;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Value;
import tools.jackson.databind.MappingIterator;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class PartitionCsvReader extends AbstractItemCountingItemStreamItemReader<Map<String, String>> {

    @Value("${app.delimiter:','}")
    private Character delimiter;

    private final CsvMapper csvMapper;
    private final Path inputFile;
    private @Nullable MappingIterator<Map<String, String>> csvIterator;

    public PartitionCsvReader(CsvMapper csvMapper, Path inputFile) {
        this.csvMapper = csvMapper;
        this.inputFile = inputFile;
        setName("partitionCsvReader");
    }

    @Override
    protected void doOpen() throws Exception {
        var schema = CsvSchema.emptySchema()
                .withHeader()
                .withColumnSeparator(delimiter);

        csvIterator = csvMapper.readerFor(Map.class)
                .with(schema)
                .readValues(Files.newInputStream(inputFile));
    }

    @Override
    protected @Nullable Map<String, String> doRead() {
        if (csvIterator == null) {
            return null;
        }
        return csvIterator.hasNext() ? csvIterator.next() : null;
    }

    @Override
    protected void doClose() {
        if (csvIterator != null) {
            csvIterator.close();
            csvIterator = null;
        }
    }
}

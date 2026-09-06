package com.my.core.csv;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import tools.jackson.databind.MappingIterator;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvSchema;

import java.util.Map;

@Component
@StepScope
public class CsvReader extends AbstractItemCountingItemStreamItemReader<Map<String, String>> {
    @Value("${app.delimiter:','}")
    private Character delimiter;

    private final CsvMapper csvMapper;
    private final Resource resource;

    private @Nullable MappingIterator<Map<String, String>> csvIterator;

    public CsvReader(CsvMapper csvMapper, Resource feedResource) {
        this.csvMapper = csvMapper;
        this.resource = feedResource;
    }

    @Override
    protected void doOpen() throws Exception {
        var schema = CsvSchema.emptySchema()
                .withHeader()
                .withColumnSeparator(delimiter);

        csvIterator = csvMapper.readerFor(Map.class)
            .with(schema)
            .readValues(resource.getInputStream());
    }

    @Override
    protected @Nullable Map<String, String> doRead() {
        if (csvIterator == null) {
            return null;
        }

        if (csvIterator.hasNext()) {
            return csvIterator.next();
        }

        return null;
    }

    @Override
    protected void doClose() {
        if (csvIterator != null) {
            csvIterator.close();
            csvIterator = null;
        }
    }
}

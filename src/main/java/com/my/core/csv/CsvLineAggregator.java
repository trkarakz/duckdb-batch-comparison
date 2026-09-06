package com.my.core.csv;

import org.springframework.batch.infrastructure.item.file.transform.ExtractorLineAggregator;
import tools.jackson.dataformat.csv.CsvMapper;
import tools.jackson.dataformat.csv.CsvSchema;
import tools.jackson.dataformat.csv.CsvWriteFeature;

import java.util.Arrays;

public class CsvLineAggregator<T> extends ExtractorLineAggregator<T> {
    private final CsvMapper csvMapper;
    private final CsvSchema schema;

    public CsvLineAggregator(CsvMapper csvMapper) {
        this.schema = CsvSchema.emptySchema()
                .withoutHeader()
                .withColumnSeparator(',');
        this.csvMapper = csvMapper;
    }

    @Override
    protected String doAggregate(Object [] fields) {
        var stringFields = Arrays.stream(fields)
            .map(String::valueOf)
            .toArray(String[]::new);

        var writer = csvMapper.writer(schema)
            .with(CsvWriteFeature.STRICT_CHECK_FOR_QUOTING);

        return writer.writeValueAsString(stringFields).stripTrailing();
    }
}

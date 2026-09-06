package com.my.core.csv;

import org.springframework.batch.infrastructure.item.file.transform.FieldExtractor;

public class FeedRowToCsvExtractor implements FieldExtractor<FeedRow> {

    @Override
    public Object[] extract(FeedRow item) {
        return new Object[]{
                item.modelName(),
                item.price()
        };
    }
}

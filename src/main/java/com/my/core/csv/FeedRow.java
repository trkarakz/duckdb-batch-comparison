package com.my.core.csv;

import java.math.BigDecimal;

public record FeedRow(
        String modelName,
        BigDecimal price
) {}
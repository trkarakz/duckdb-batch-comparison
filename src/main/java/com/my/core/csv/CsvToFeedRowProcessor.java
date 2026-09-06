package com.my.core.csv;

import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Component
public class CsvToFeedRowProcessor {

    private static final List<String> PRICE_COMPONENT_COLUMNS = List.of(
            "fsc_discountPrice",
            "fsc_bpm",
            "fsc_extColorPrice",
            "fsc_deliveryCharges",
            "fsc_AdministrativeExpenses",
            "fsc_EnvironmentalTax",
            "fsc_StandardDeliveryPackage",
            "fsc_LuxuryDeliveryPackage",
            "fsc_ExtendedWarranty6thYear",
            "fsc_ExtendedWarranty6thAnd7thYear",
            "fsc_ExtendedWarrantyForQuickDecissers",
            "fsc_extColorPriceLeasePrivate",
            "fsc_extColorPriceLeaseCompany"
    );

    public FeedRow mapLine(Map<String, String> row) {
        var modelName = trimToNull(row.get("model_commercieleModelnaam"));
        if (modelName == null) {
            modelName = "";
        }

        var basePrice = parseEuro(row.get("fsc_fscPrice"));
        if (basePrice == null) {
            basePrice = BigDecimal.ZERO;
        }

        var componentsSum = PRICE_COMPONENT_COLUMNS.stream()
                .map(row::get)
                .map(this::parseEuro)
                .filter(java.util.Objects::nonNull)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        var calculatedPrice = basePrice.subtract(componentsSum);

        return new FeedRow(modelName, calculatedPrice);
    }

    private @Nullable BigDecimal parseEuro(String raw) {
        var cleaned = raw
                .replace('\u00A0', ' ')
                .replace("€", "")
                .replaceAll("[^0-9,.-]", "")
                .trim();

        if (cleaned.isEmpty()) {
            return null;
        }

        if (cleaned.contains(",") && cleaned.contains(".")) {
            cleaned = cleaned.replace(".", "").replace(",", ".");
        } else if (cleaned.contains(",")) {
            cleaned = cleaned.replace(",", ".");
        }

        try {
            return new BigDecimal(cleaned);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private @Nullable String trimToNull(String value) {
        var v = value.trim();
        return v.isEmpty() ? null : v;
    }
}

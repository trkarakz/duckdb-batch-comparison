package com.my.core.tasklet;

import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.core.io.Resource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;

public class DuckDbTransformTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(DuckDbTransformTasklet.class);

    private final String duckdbUrl;
    private final Resource feedResource;

    public DuckDbTransformTasklet(String duckdbUrl, Resource feedResource) {
        this.duckdbUrl = duckdbUrl;
        this.feedResource = feedResource;
    }

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution, @NonNull ChunkContext chunkContext) throws Exception {
        var feedPath = feedResource.getFilePath().toAbsolutePath();
        var output = "./duckDbOutput.csv";
        var sql = """
                COPY (
                    WITH src AS (
                        SELECT *
                        FROM read_csv('__FEED__', header = true, all_varchar = true)
                    )
                    SELECT
                        coalesce(trim(model_commercieleModelnaam), '') AS model,
                        (
                            coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_fscPrice, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_discountPrice, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_bpm, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_extColorPrice, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_deliveryCharges, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_AdministrativeExpenses, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_EnvironmentalTax, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_StandardDeliveryPackage, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_LuxuryDeliveryPackage, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_ExtendedWarranty6thYear, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_ExtendedWarranty6thAnd7thYear, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_ExtendedWarrantyForQuickDecissers, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_extColorPriceLeasePrivate, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                            - coalesce(TRY_CAST(replace(replace(regexp_replace(fsc_extColorPriceLeaseCompany, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') AS DECIMAL(18,2)), 0)
                        ) AS price
                    FROM src
                ) TO '__OUTPUT__' (FORMAT CSV, HEADER true)
                """
                .replace("__FEED__", feedPath.toString().replace("\\", "/"))
                .replace("__OUTPUT__", output.replace("\\", "/"));

        try (Connection conn = DriverManager.getConnection(duckdbUrl);
             Statement st = conn.createStatement()) {

            long threads = queryLong(st, "SELECT current_setting('threads')");
            long rows = queryLong(st, "SELECT count(*) FROM read_csv('" + feedPath + "', header = true)");

            long start = System.nanoTime();
            st.execute(sql);
            double seconds = (System.nanoTime() - start) / 1e9;

            long outputRows = queryLong(st,
                    "SELECT count(*) FROM read_csv('" + output + "', header = true)");

            log.info("DuckDB transform: {} input rows -> {} output rows in {} s on {} threads",
                    rows, outputRows, String.format(Locale.US, "%.2f", seconds), threads);
        }
        return RepeatStatus.FINISHED;
    }

    private static long queryLong(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
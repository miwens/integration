package ru.yana.integration;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Component
public class ReportRoute extends RouteBuilder {

    private static final Logger log = LoggerFactory.getLogger(ReportRoute.class);

    @Autowired
    private JdbcTemplate jdbc;

    @Value("${report.api.url}")
    private String reportUrl;

    @Override
    public void configure() throws Exception {
        from("quartz://reportSync?trigger.repeatInterval={{sync.timer.period}}")
            .routeId("db-to-report")
            .log("Starting report generation")
            .process("authProcessor")
            .process(this::buildCsvAndUpload)
            .log("Report generation completed");
    }

    private void buildCsvAndUpload(Exchange exchange) throws Exception {
        String token = exchange.getIn().getHeader("BearerToken", String.class);

        List<Map<String, Object>> rows = jdbc.queryForList(
            "SELECT spare_code, spare_name, spare_description, spare_type, " +
            "spare_status, price, quantity, updated_at FROM spares_history ORDER BY spare_code"
        );

        StringBuilder csv = new StringBuilder();
        for (Map<String, Object> row : rows) {
            csv.append(escapeCsv(row.get("spare_code"))).append(";")
               .append(escapeCsv(row.get("spare_name"))).append(";")
               .append(escapeCsv(row.get("spare_description"))).append(";")
               .append(escapeCsv(row.get("spare_type"))).append(";")
               .append(escapeCsv(row.get("spare_status"))).append(";")
               .append(escapeCsv(row.get("price"))).append(";")
               .append(escapeCsv(row.get("quantity"))).append(";")
               .append(formatUpdatedAt(row.get("updated_at")))
               .append("\n");
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(reportUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "text/csv; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(csv.toString(), StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        log.info("Report uploaded: status={}, rows={}", response.statusCode(), rows.size());
    }

    private String formatUpdatedAt(Object updatedAt) {
        if (updatedAt == null) return "";
        if (updatedAt instanceof java.sql.Timestamp) {
            return updatedAt.toString().replace(" ", "T");
        }
        return updatedAt.toString();
    }

    private String escapeCsv(Object value) {
        if (value == null) return "";
        String str = value.toString();
        if (str.contains(";") || str.contains("\n") || str.contains("\"")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }
}

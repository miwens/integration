package ru.yana.integration;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

@Component
public class ReportRoute extends RouteBuilder {

    @Autowired
    private JdbcTemplate jdbc;

    @Value("${report.api.url}")
    private String reportUrl;

    @Override
    public void configure() throws Exception {
        from("quartz://reportSync?trigger.repeatInterval={{sync.timer.period}}")
            .routeId("db-to-report")
            .log("=== START Report sync ===")
            .process("authProcessor")
            .process(this::buildCsvAndUpload)
            .log("=== END Report sync ===");
    }

    private void buildCsvAndUpload(Exchange exchange) throws Exception {
        String token = exchange.getIn().getHeader("BearerToken", String.class);

        var rows = jdbc.queryForList(
            "SELECT spare_code, spare_name, spare_description, spare_type, " +
            "spare_status, price, quantity, updated_at FROM spares_history ORDER BY spare_code"
        );

        StringBuilder csv = new StringBuilder();
        for (var r : rows) {
            csv.append(safe(r.get("spare_code"))).append(";")
               .append(safe(r.get("spare_name"))).append(";")
               .append(safe(r.get("spare_description"))).append(";")
               .append(safe(r.get("spare_type"))).append(";")
               .append(safe(r.get("spare_status"))).append(";")
               .append(safe(r.get("price"))).append(";")
               .append(safe(r.get("quantity"))).append(";")
               .append(r.get("updated_at") == null ? "" : r.get("updated_at").toString())
               .append("\n");
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(reportUrl))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "text/csv; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(csv.toString(), StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> resp = HttpClient.newHttpClient()
                .send(req, HttpResponse.BodyHandlers.ofString());

        log.info("Report upload status: {} | rows: {}", resp.statusCode(), rows.size());
    }

    private String safe(Object o) {
        if (o == null) return "";
        String s = o.toString();
        if (s.contains(";") || s.contains("\n")) s = "\"" + s.replace("\"", "\"\"") + "\"";
        return s;
    }
}

package ru.yana.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Component
public class CmsSyncRoute extends RouteBuilder {

    private static final Logger log = LoggerFactory.getLogger(CmsSyncRoute.class);

    @Autowired
    private JdbcTemplate jdbc;

    @Value("${cms.api.url}")
    private String cmsUrl;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure() throws Exception {
        from("quartz://cmsSync?trigger.repeatInterval={{sync.timer.period}}")
            .routeId("cms-to-db")
            .log("Starting CMS synchronization")
            .process("authProcessor")
            .process(this::syncAllPages)
            .log("CMS synchronization completed");
    }

    private void syncAllPages(Exchange exchange) throws Exception {
        String basicAuth = exchange.getIn().getHeader("BasicAuth", String.class);
        Set<String> cmsCodes = new HashSet<>();

        int page = 0;
        int size = 10;

        while (true) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(cmsUrl + "?page=" + page + "&size=" + size))
                    .header("Authorization", basicAuth)
                    .GET()
                    .build();

            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("CMS returned error " + response.statusCode());
            }

            JsonNode root = mapper.readTree(response.body());
            JsonNode items = extractItems(root);

            if (items == null || items.isEmpty()) {
                break;
            }

            for (JsonNode item : items) {
                String code = extractField(item, "spareCode", "code", "id");
                if (code != null) {
                    cmsCodes.add(code);
                    upsert(item, code);
                }
            }

            if (root.has("totalPages") && page >= root.get("totalPages").asInt() - 1) break;
            if (items.size() < size) break;
            page++;
        }

        log.info("Synced {} unique spare parts from CMS", cmsCodes.size());

        if (!cmsCodes.isEmpty()) {
            markArchived(cmsCodes);
        }
    }

    private JsonNode extractItems(JsonNode root) {
        for (String key : new String[]{"content", "items", "data", "spares", "records"}) {
            if (root.has(key) && root.get(key).isArray()) {
                return root.get(key);
            }
        }
        return root.isArray() ? root : null;
    }

    private void upsert(JsonNode item, String code) {
        String updatedAt = extractField(item, "updatedAt");
        if (updatedAt == null) {
            updatedAt = LocalDateTime.now().toString();
        }

        jdbc.update(
            "INSERT INTO spares_history (spare_code, spare_name, spare_description, spare_type, " +
            "spare_status, price, quantity, updated_at, last_seen_at, is_archived) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), FALSE) " +
            "ON CONFLICT (spare_code) DO UPDATE SET " +
            "spare_name = EXCLUDED.spare_name, " +
            "spare_description = EXCLUDED.spare_description, " +
            "spare_type = EXCLUDED.spare_type, " +
            "spare_status = EXCLUDED.spare_status, " +
            "price = EXCLUDED.price, " +
            "quantity = EXCLUDED.quantity, " +
            "updated_at = EXCLUDED.updated_at, " +
            "last_seen_at = NOW(), " +
            "is_archived = FALSE",

            code,
            extractField(item, "spareName"),
            extractField(item, "spareDescription"),
            extractField(item, "spareType"),
            extractField(item, "spareStatus"),
            item.has("price") ? item.get("price").asText() : null,
            item.has("quantity") ? item.get("quantity").asInt() : null,
            updatedAt
        );
    }

    private void markArchived(Set<String> cmsCodes) {
        String placeholders = String.join(",", cmsCodes.stream().map(c -> "?").toArray(String[]::new));
        jdbc.update(
            "UPDATE spares_history SET is_archived = TRUE, archived_at = NOW() " +
            "WHERE spare_code NOT IN (" + placeholders + ") AND is_archived = FALSE",
            cmsCodes.toArray()
        );
    }

    private String extractField(JsonNode node, String... keys) {
        for (String key : keys) {
            if (node.has(key) && !node.get(key).isNull()) {
                return node.get(key).asText();
            }
        }
        return null;
    }
}

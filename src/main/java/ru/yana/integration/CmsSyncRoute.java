package ru.yana.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

@Component
public class CmsSyncRoute extends RouteBuilder {

    @Autowired
    private JdbcTemplate jdbc;

    @Value("${cms.api.url}")
    private String cmsUrl;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure() throws Exception {
        from("quartz://cmsSync?trigger.repeatInterval={{sync.timer.period}}")
            .routeId("cms-to-db")
            .log("=== START CMS sync (PostgreSQL) ===")
            .process("authProcessor")
            .process(this::syncAllPages)
            .log("=== END CMS sync ===");
    }

  private void syncAllPages(Exchange exchange) throws Exception {
    String basic = exchange.getIn().getHeader("BasicAuth", String.class);
    Set<String> cmsCodes = new HashSet<>();

    int page = 0;
    int size = 10;

    while (true) {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(cmsUrl + "?page=" + page + "&size=" + size))
                .header("Authorization", basic)
                .GET()
                .build();

        HttpResponse<String> resp = HttpClient.newHttpClient()
                .send(req, HttpResponse.BodyHandlers.ofString());

        // ✅ ДОБАВЛЕНО: проверка статуса и логирование
        System.out.println("=== CMS DEBUG page=" + page + " ===");
        System.out.println("HTTP Status: " + resp.statusCode());
        System.out.println("Response Body: " +
                (resp.body().length() > 500 ? resp.body().substring(0, 500) + "..." : resp.body()));
        System.out.println("==========================");

        if (resp.statusCode() != 200) {
            throw new RuntimeException("CMS вернула ошибку " + resp.statusCode()
                    + ": " + resp.body());
        }

        JsonNode root = mapper.readTree(resp.body());

        // ✅ ДОБАВЛЕНО: перебираем все возможные имена поля с массивом
        JsonNode items = null;
        for (String key : new String[]{"content", "items", "data", "spares", "records"}) {
            if (root.has(key) && root.get(key).isArray()) {
                items = root.get(key);
                break;
            }
        }
        if (items == null && root.isArray()) items = root; // если корень — массив

        if (items == null || items.isEmpty()) {
            System.out.println("No items found on page " + page);
            break;
        }

        for (JsonNode item : items) {
            String code = firstNonNull(item, "spareCode", "code", "id");
            if (code == null) continue;
            cmsCodes.add(code);
            upsert(item, code);
        }

        if (root.has("totalPages") && page >= root.get("totalPages").asInt() - 1) break;
        if (items.size() < size) break;
        page++;
    }

    System.out.println("Total unique codes from CMS: " + cmsCodes.size());

    if (!cmsCodes.isEmpty()) {
        String placeholders = String.join(",", cmsCodes.stream().map(c -> "?").toArray(String[]::new));
        jdbc.update(
            "UPDATE spares_history SET is_archived = TRUE, archived_at = NOW() " +
            "WHERE spare_code NOT IN (" + placeholders + ") AND is_archived = FALSE",
            cmsCodes.toArray()
        );
    }
}
    private void upsert(JsonNode item, String code) {
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
            text(item, "spareName", "name"),
            text(item, "spareDescription", "description"),
            text(item, "spareType", "type"),
            text(item, "spareStatus", "status"),
            item.has("price") ? item.get("price").asDouble() : null,
            item.has("quantity") ? item.get("quantity").asInt() : null,
            parseTs(text(item, "updatedAt"))
        );
    }

    private String text(JsonNode n, String... keys) {
        for (String k : keys) if (n.has(k) && !n.get(k).isNull()) return n.get(k).asText();
        return null;
    }

    private String firstNonNull(JsonNode n, String... keys) { return text(n, keys); }

    private Timestamp parseTs(String s) {
        if (s == null) return Timestamp.valueOf(LocalDateTime.now());
        try { return Timestamp.valueOf(s.replace("T", " ").split("\\.")[0]); }
        catch (Exception e) { return Timestamp.valueOf(LocalDateTime.now()); }
    }
}

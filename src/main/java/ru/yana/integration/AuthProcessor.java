package ru.yana.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

@Component("authProcessor")
public class AuthProcessor implements Processor {

    private static final Logger log = LoggerFactory.getLogger(AuthProcessor.class);

    @Value("${token.api.url}")
    private String tokenUrl;

    @Value("${api.username}")
    private String username;

    @Value("${api.password}")
    private String password;

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void process(Exchange exchange) throws Exception {
        String token = getBearerToken();
        String basicAuth = "Basic " + Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes());

        exchange.getIn().setHeader("BearerToken", token);
        exchange.getIn().setHeader("BasicAuth", basicAuth);
    }

    private String getBearerToken() throws Exception {
        String body = String.format("{\"username\":\"%s\",\"password\":\"%s\"}",
                username, password);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            log.error("Auth failed with status {}: {}", response.statusCode(), response.body());
            throw new RuntimeException("Authentication failed: " + response.statusCode());
        }

        JsonNode json = mapper.readTree(response.body());
        if (json.has("token")) return json.get("token").asText();
        if (json.has("access_token")) return json.get("access_token").asText();

        throw new RuntimeException("Token not found in response");
    }
}

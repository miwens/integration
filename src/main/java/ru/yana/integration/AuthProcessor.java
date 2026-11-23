package ru.yana.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

@Component("authProcessor")
public class AuthProcessor implements Processor {

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
        String basic = "Basic " + Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes());

        exchange.getIn().setHeader("BearerToken", token);
        exchange.getIn().setHeader("BasicAuth", basic);
    }

    private String getBearerToken() throws Exception {
        String body = String.format("{\"username\":\"%s\",\"password\":\"%s\"}",
                username, password);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> resp = HttpClient.newHttpClient()
                .send(req, HttpResponse.BodyHandlers.ofString());

        JsonNode json = mapper.readTree(resp.body());
        if (json.has("token")) return json.get("token").asText();
        if (json.has("access_token")) return json.get("access_token").asText();

        throw new RuntimeException("Token not found: " + resp.body());
    }
}

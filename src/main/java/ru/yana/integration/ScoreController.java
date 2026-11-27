package ru.yana.integration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@RestController
public class ScoreController {

    @Value("${token.api.url}")
    private String tokenUrl;

    @Value("${api.student-id}")
    private String studentId;

    @GetMapping("/my-score")
    public String getScore() throws Exception {
        String baseUrl = tokenUrl.replace("/auth/token", "");
        String url = baseUrl + "/students/" + studentId + "/results";

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        return response.body();
    }
}

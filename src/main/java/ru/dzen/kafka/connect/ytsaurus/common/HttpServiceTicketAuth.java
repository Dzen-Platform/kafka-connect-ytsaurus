package ru.dzen.kafka.connect.ytsaurus.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.rpc.ServiceTicketAuth;

public class HttpServiceTicketAuth implements ServiceTicketAuth {

  private static final Logger log = LoggerFactory.getLogger(HttpServiceTicketAuth.class);
  private final String serviceUrl;
  private final ObjectMapper mapper;

  public HttpServiceTicketAuth(String serviceUrl) {
    this.serviceUrl = serviceUrl;
    this.mapper = new ObjectMapper();
    log.debug("HttpServiceTicketAuth initialized.");
  }

  @Override
  public String issueServiceTicket() {
    StringBuilder response = new StringBuilder();
    log.debug("Requesting service ticket.");

    try {
      URL url = new URL(serviceUrl);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();

      connection.setRequestMethod("GET");
      connection.setRequestProperty("Accept", "application/json");

      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        log.error("Failed: HTTP error code : {}", connection.getResponseCode());
        throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
      }

      BufferedReader br = new BufferedReader(new InputStreamReader((connection.getInputStream())));

      String output;
      while ((output = br.readLine()) != null) {
        response.append(output);
      }

      connection.disconnect();
    } catch (IOException e) {
      log.error("IOException occurred while making a GET request.", e);
      throw new RuntimeException(e);
    }

    try {
      JsonNode jsonResponse = mapper.readTree(response.toString());
      if (jsonResponse.has("error")) {
        log.error("Error received from service: {}", jsonResponse.get("error").asText());
        throw new RuntimeException(jsonResponse.get("error").asText());
      } else if (jsonResponse.has("ticket")) {
        log.debug("Received service ticket from service.");
        return jsonResponse.get("ticket").asText();
      } else {
        log.error("Invalid ticket response: {}", jsonResponse);
        throw new RuntimeException("Invalid ticket response: " + jsonResponse);
      }
    } catch (IOException e) {
      log.error("Error parsing JSON response from service.", e);
      throw new RuntimeException("Error parsing JSON response: " + e.getMessage());
    }
  }
}

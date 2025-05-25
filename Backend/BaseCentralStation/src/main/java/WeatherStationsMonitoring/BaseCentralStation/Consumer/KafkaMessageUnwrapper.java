package WeatherStationsMonitoring.BaseCentralStation.Consumer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaMessageUnwrapper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String unwrapHeader(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode header = root.path("header");

            if (header.isMissingNode()) {
                throw new IllegalArgumentException("Missing 'header' field in message.");
            }

            return objectMapper.writeValueAsString(header);

        } catch (Exception e) {
            System.err.println("Error unwrapping header: " + e.getMessage());
            return "{}"; // Return empty JSON object on failure
        }
    }
}

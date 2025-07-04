package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class WeatherStationMock {

    public static void main(String[] args) {
//        HttpClient client = HttpClient.newHttpClient();
//        Random rand = new Random();
//        String endpoint = "http://localhost:8080/weatherMonitoring/BaseCentralStation";
        String endpoint = "http://192.168.49.2:30080/weatherMonitoring/BaseCentralStation";
         final int TOTAL_MESSAGES = 10000; // per station
        final int STATIONS = 2;
//        final String endpoint = "http://localhost:8080/weatherMonitoring/BaseCentralStation";
        final HttpClient client = HttpClient.newHttpClient();
        final Random rand = new Random();

        // Create an executor with as many threads as stations
        ExecutorService executor = Executors.newFixedThreadPool(STATIONS);

        // Launch a thread per station
        for (long stationId = 1; stationId <= STATIONS; stationId++) {
            long finalStationId = stationId;
            executor.submit(() -> sendMessages(client, rand, endpoint, finalStationId, TOTAL_MESSAGES));
        }

        // Shutdown executor after submitting tasks and wait for them to complete
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.out.println("Timeout: Some tasks did not finish within the hour.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Sends the specified number of messages asynchronously for a given station.
     */
    private static void sendMessages(HttpClient client, Random rand, String endpoint,
                                     long stationId, int totalMessages) {
        long messageNo = 1;
        int sent = 0;

        while (sent < totalMessages) {
            // Determine battery status
            int batteryRoll = rand.nextInt(100);
            String batteryStatus = batteryRoll < 30 ? "low" :
                    batteryRoll < 70 ? "medium" : "high";

            // Simulate a 10% chance to "drop" the message, counting it as sent
            if (rand.nextDouble() < 0.1) {
                messageNo++;
                sent++;
                continue;
            }

            // Build the weather status and JSON
            WeatherStatus status = new WeatherStatus(stationId, messageNo, batteryStatus, rand);
            String json = status.toJson();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();

            // Send the request asynchronously
            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        // Optionally log or process the response
                        // System.out.printf("[Station %d] Sent message #%d: Status %d%n", stationId, messageNo, response.statusCode());
                    })
                    .exceptionally(e -> {
//                        System.err.printf("[Station %d] Failed to send message #%d: %s%n", stationId, messageNo, e.getMessage());
                        return null;
                    });

            messageNo++;
            sent++;
        }
    }
}

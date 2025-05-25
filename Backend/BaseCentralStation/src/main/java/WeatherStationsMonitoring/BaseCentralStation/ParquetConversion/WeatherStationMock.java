package WeatherStationsMonitoring.BaseCentralStation.ParquetConversion;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Random;

public class WeatherStationMock {

    public static void main(String[] args) {
        HttpClient client = HttpClient.newHttpClient();
        Random rand = new Random();
//        String endpoint = "http://localhost:8080/weatherMonitoring/BaseCentralStation";
        String endpoint = "http://192.168.49.2:30080/weatherMonitoring/BaseCentralStation";

        final int TOTAL_MESSAGES = 10;
        final int STATIONS = 3;

        for (long stationId = 1; stationId <= STATIONS; stationId++) {
            long s_no = 1;
            int sent = 0;

            while (sent < TOTAL_MESSAGES) {
                // Always increment message counter (including dropped)
                int batteryRoll = rand.nextInt(100);
                String batteryStatus = batteryRoll < 30 ? "low" :
                        batteryRoll < 70 ? "medium" : "high";

                if (rand.nextDouble() < 0.1) {
                    System.out.printf("[Station %d] Dropped message #%d\n", stationId, s_no);
                    s_no++;
                    sent++;
                    sleepOneSecond();
                    continue;
                }

                WeatherStatus status = new WeatherStatus(stationId, s_no, batteryStatus, rand);
                String json = status.toJson();
                System.out.println(json);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    System.out.printf("[Station %d] Sent message #%d: Status %d\n", stationId, s_no, response.statusCode());
                } catch (Exception e) {
                    System.err.printf("[Station %d] Failed to send message #%d: %s\n", stationId, s_no, e.getMessage());
                }

                s_no++;
                sent++;
                sleepOneSecond();
            }
        }
    }

    private static void sleepOneSecond() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
    }
}

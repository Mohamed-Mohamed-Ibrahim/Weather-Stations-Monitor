package com.example.DDIAProject;

import com.google.gson.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

public class StationConsumer {

    private static final Gson gson = new Gson();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    public static void main(String[] args) {

        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(
                Station.Topics.WEATHER_RAW.toString(),
                Station.Topics.WEATHER_DROPPED.toString(),
                Station.Topics.WEATHER_RAIN.toString(),
                Station.Topics.WEATHER_ARCHIVE.toString(),
                Station.Topics.WEATHER_INVALID.toString()
        ));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));


                for (ConsumerRecord<String, String> record : records) {


                    try {

                        String decryptedJson = decryptAndUnwrap(record.value());
                        JsonObject payload = JsonParser.parseString(decryptedJson).getAsJsonObject();
                        System.out.println(payload);
                        if (isValidStationMessage(payload)) {

//                            postToCentralStation(decryptedJson);
                        } else {
                            System.err.println("Invalid message structure, skipping: " + decryptedJson);
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }

            }
        }


    private static String decryptAndUnwrap(String wrappedJson) throws Exception {
        JsonObject root = JsonParser.parseString(wrappedJson).getAsJsonObject();

        String encrypted = root.get("encrypted_payload").getAsString();

        String decryptedJson = EncryptionUtil.decrypt(encrypted);


        return decryptedJson;
    }

    private static boolean isValidStationMessage(JsonObject json) {
        try {
            JsonObject weather = json.getAsJsonObject("weather");
            return json.has("station_id") &&
                    json.has("s_no") &&
                    json.has("battery_status") &&
                    json.has("status_timestamp") &&
                    weather.has("humidity") &&
                    weather.has("temperature") &&
                    weather.has("wind_speed");
        } catch (Exception e) {
            return false;
        }
    }




    private static void postToCentralStation(String stationJson) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI("http://localhost:8080/weatherMonitoring/BaseCentralStation"))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(stationJson))
                .build();


        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.printf("POST %s → %d%n", stationJson, response.statusCode());
    }
}

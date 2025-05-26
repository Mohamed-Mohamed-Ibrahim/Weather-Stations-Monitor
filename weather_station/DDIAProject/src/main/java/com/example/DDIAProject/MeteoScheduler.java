package com.example.DDIAProject;

import com.google.gson.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.*;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
@EnableScheduling
public class MeteoScheduler {

    private static final Gson gson = new Gson();
    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private final KafkaProducer<String, String> producer;
    private final Random rand = new Random();

    public MeteoScheduler(@Value("${kafka.bootstrap}") String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Scheduled(cron = "0 4 2 * * *", zone = "Africa/Cairo")
    public void fetchAndPublish() throws Exception {

        String url = "https://api.open-meteo.com/v1/forecast"
                + "?latitude=31.200092&longitude=29.918739"
                + "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";
        HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(url))
                .GET()
                .build();
        String resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString()).body();

        JsonObject root = JsonParser.parseString(resp).getAsJsonObject();
        JsonObject hourly = root.getAsJsonObject("hourly");
        JsonArray times = hourly.getAsJsonArray("time");
        JsonArray temps = hourly.getAsJsonArray("temperature_2m");
        JsonArray hums = hourly.getAsJsonArray("relative_humidity_2m");
        JsonArray winds = hourly.getAsJsonArray("wind_speed_10m");


        List<JsonObject> allMessages = new ArrayList<>(24);
        for (int i = 0; i < 24; i++) {
            JsonObject msg = new JsonObject();
            msg.addProperty("station_id", 11L);
            msg.addProperty("s_no", i + 1);
            msg.addProperty("status_timestamp",
                    ZonedDateTime.parse(times.get(i).getAsString() + "Z",
                            DateTimeFormatter.ISO_DATE_TIME).toEpochSecond());
            JsonObject weather = new JsonObject();
            weather.addProperty("temperature", temps.get(i).getAsInt());
            weather.addProperty("humidity", hums.get(i).getAsInt());
            weather.addProperty("wind_speed", winds.get(i).getAsInt());
            msg.add("weather", weather);
            allMessages.add(msg);
        }

        List<String> batteries = new ArrayList<>();
        batteries.addAll(Collections.nCopies(6, "low"));
        batteries.addAll(Collections.nCopies(8, "medium"));
        batteries.addAll(Collections.nCopies(6, "high"));
        Collections.shuffle(batteries);

        List<Integer> allMessageNumbers = new ArrayList<>();
        for (int i = 1; i <= 24; i++) allMessageNumbers.add(i);

        List<Integer> droppedNumbers = new ArrayList<>();
        List<Integer> invalidNumbers = new ArrayList<>();
        List<Integer> rawNumbers = new ArrayList<>();

        while (droppedNumbers.size() < 2) {
            int candidate = rand.nextInt(24) + 1;
            if (!droppedNumbers.contains(candidate)) {
                droppedNumbers.add(candidate);
            }
        }

        while (invalidNumbers.size() < 4) {
            int candidate = rand.nextInt(24) + 1;
            if (!droppedNumbers.contains(candidate) && !invalidNumbers.contains(candidate)) {
                invalidNumbers.add(candidate);
            }
        }

        for (int i = 1; i <= 24; i++) {
            if (!droppedNumbers.contains(i) && !invalidNumbers.contains(i)) {
                rawNumbers.add(i);
            }
        }

        List<MessageWrapper> allToSend = new ArrayList<>();
        int batteryIndex = 0;

        for (JsonObject msg : allMessages) {
            long sNo = msg.get("s_no").getAsLong();

            if (droppedNumbers.contains((int)sNo)) {
                msg.addProperty("battery_status", batteries.get(batteryIndex++));
                allToSend.add(new MessageWrapper(msg, "dropped"));
            }
            else if (invalidNumbers.contains((int)sNo)) {
                allToSend.add(new MessageWrapper(msg, "invalid"));
            }
            else {
                msg.addProperty("battery_status", batteries.get(batteryIndex++));
                allToSend.add(new MessageWrapper(msg, "raw"));
            }
        }

        allToSend.sort(Comparator.comparingLong(w -> w.message.get("s_no").getAsLong()));

        for (MessageWrapper wrapper : allToSend) {
            String topic = switch (wrapper.type) {
                case "invalid" -> Station.Topics.WEATHER_INVALID.toString();
                case "dropped" -> Station.Topics.WEATHER_DROPPED.toString();
                default -> Station.Topics.WEATHER_RAW.toString();
            };

            String wrapped = wrapAndEncryptMessage(wrapper.message);
            producer.send(new ProducerRecord<>(topic, wrapped));
        }
        producer.flush();
    }

    private String wrapAndEncryptMessage(JsonObject fullMessage) throws Exception {
        JsonElement stationId = fullMessage.get("station_id");
        JsonElement serialNo = fullMessage.get("s_no");
        JsonElement humidity = fullMessage.getAsJsonObject("weather").get("humidity");

        JsonObject header = new JsonObject();
        header.add("station_id", stationId);
        header.add("s_no", serialNo);
        header.addProperty("timestamp", System.currentTimeMillis() / 1000L);
        header.add("humidity", humidity);

        String payloadJson = gson.toJson(fullMessage);
        String encryptedPayload = EncryptionUtil.encrypt(payloadJson);

        JsonObject wrapper = new JsonObject();
        wrapper.add("header", header);
        wrapper.addProperty("encrypted_payload", encryptedPayload);

        return gson.toJson(wrapper);
    }
    private static class MessageWrapper {
        JsonObject message;
        String type;

        MessageWrapper(JsonObject message, String type) {
            this.message = message;
            this.type = type;
        }
    }
}
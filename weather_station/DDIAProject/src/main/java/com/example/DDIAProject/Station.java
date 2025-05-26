package com.example.DDIAProject;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class Station {

    private final long stationId;
    private long serialNo;
    private List<BatteryStatus> batterySchedule;
    private int dropIndex;
    private int emittedCount;
    private static final Random rand = new Random();
    private static final Gson gson = new Gson();
    private static KafkaProducer<String, String> kafkaProducer;

    public enum BatteryStatus {
        LOW, MEDIUM, HIGH
    }

    public enum Topics {
        WEATHER_RAW("weather-raw"),
        WEATHER_DROPPED("weather-dropped"),
        WEATHER_RAIN("weather-rain"),
        WEATHER_ARCHIVE("weather-archive"),
        WEATHER_INVALID("weather-invalid");


        private final String name;
        Topics(String name) { this.name = name; }
        @Override public String toString() { return name; }
    }


    public Station(long stationId) {
        this.stationId = stationId;
        this.serialNo = 1;
        this.emittedCount = 0;
        prepareNextRound();
    }

    private void prepareNextRound() {
        batterySchedule = new ArrayList<>();
        batterySchedule.addAll(Collections.nCopies(3, BatteryStatus.LOW));
        batterySchedule.addAll(Collections.nCopies(4, BatteryStatus.MEDIUM));
        batterySchedule.addAll(Collections.nCopies(3, BatteryStatus.HIGH));
        Collections.shuffle(batterySchedule);
        dropIndex = rand.nextInt(10);
        emittedCount = 0;
    }

    private static class Weather {
        int humidity, temperature, windSpeed;

        public Weather(int humidity, int temperature, int windSpeed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.windSpeed = windSpeed;
        }
    }

    private Map<String, Object> generateMessage(BatteryStatus status) {
        Map<String, Object> message = new LinkedHashMap<>();
        message.put("station_id", stationId);
        message.put("s_no", serialNo++);
        message.put("battery_status", status.name().toLowerCase());
        message.put("status_timestamp", System.currentTimeMillis() / 1000L);

        Weather weather = new Weather(
                rand.nextInt(81) + 20,
                rand.nextInt(81) + 30,
                rand.nextInt(51)
        );

        Map<String, Object> weatherMap = new HashMap<>();
        weatherMap.put("humidity", weather.humidity);
        weatherMap.put("temperature", weather.temperature);
        weatherMap.put("wind_speed", weather.windSpeed);
        message.put("weather", weatherMap);

        return message;
    }



    public static String wrapAndEncryptMessage(Map<String, Object> fullMessage) throws Exception {
        Object stationId = fullMessage.get("station_id");
        Object serialNo  = fullMessage.get("s_no");

        Map<String, Object> weather = (Map<String, Object>) fullMessage.get("weather");
        Object humidity = weather.get("humidity");

        Map<String, Object> header = new LinkedHashMap<>();
        header.put("station_id", stationId);
        header.put("s_no", serialNo);
        header.put("timestamp", System.currentTimeMillis() / 1000L);
        header.put("humidity", humidity);

        Map<String, Object> payload = new LinkedHashMap<>(fullMessage);

        String payloadJson = gson.toJson(payload);
        String encryptedPayload = EncryptionUtil.encrypt(payloadJson);

        Map<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("header", header);
        wrapper.put("encrypted_payload", encryptedPayload);

        return gson.toJson(wrapper);
    }



    public void emitNextMessage() {
        if (emittedCount >= 10) prepareNextRound();

        Map<String, Object> message = generateMessage(batterySchedule.get(emittedCount));

        try {
            String wrapped = wrapAndEncryptMessage(message);
            String topic  = (emittedCount == dropIndex)
                    ? Topics.WEATHER_DROPPED.toString()
                    : Topics.WEATHER_RAW.toString();
            kafkaProducer.send(new ProducerRecord<>(topic, wrapped));
        } catch (Exception e) {
            e.printStackTrace();
        }

        emittedCount++;
    }



    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducer = new KafkaProducer<>(props);

        List<Station> stations = new ArrayList<>();
        for (int i = 1; i <= 10; i++) stations.add(new Station(i));

        while (true) {
            for (Station station : stations) station.emitNextMessage();
            Thread.sleep(1000);
        }
    }
}

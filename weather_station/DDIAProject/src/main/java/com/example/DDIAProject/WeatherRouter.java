package com.example.DDIAProject;

import com.google.gson.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class WeatherRouter {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-router-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(Station.Topics.WEATHER_RAW.toString());

        stream.split(Named.as("branch-"))
                .branch((key, value) -> extractHeaderHumidity(value) > 70,
                        Branched.withConsumer(s -> s.to(Station.Topics.WEATHER_RAIN.toString())))
                .defaultBranch(Branched.withConsumer(s -> s.to(Station.Topics.WEATHER_ARCHIVE.toString())));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static int extractHeaderHumidity(String json) {
        try {
            JsonObject root   = JsonParser.parseString(json).getAsJsonObject();
            JsonObject header = root.getAsJsonObject("header");
            return header.get("humidity").getAsInt();
        } catch (Exception e) {

            return -1;
        }
    }
}
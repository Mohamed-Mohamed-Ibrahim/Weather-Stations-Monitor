package com.example.DDIAProject;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;


@SpringBootApplication
@EnableScheduling
public class DdiaProjectApplication {

	private static final Gson gson = new Gson();
	private static final HttpClient httpClient = HttpClient.newHttpClient();

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
//				.uri(new URI("http://localhost:8080/weatherMonitoring/BaseCentralStation"))
				.uri(new URI("http://base-central-station-service:8080/weatherMonitoring/BaseCentralStation"))
//				.uri(new URI("http://192.168.49.2:30080/weatherMonitoring/BaseCentralStation"))
				.timeout(Duration.ofSeconds(5))
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(stationJson))
				.build();


		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		System.out.printf("POST %s → %d%n", stationJson, response.statusCode());
	}

	public static void main(String[] args) {

		SpringApplication.run(DdiaProjectApplication.class, args);


	}

	@Bean
	CommandLineRunner commandLineRunner () {
		return args -> {
			System.out.println("Program Started");

			Properties props = new Properties();
//			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9	092");
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");
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

			long startTime = System.currentTimeMillis();
			long runDurationMillis = 60 * 1000; // 1 minute in milliseconds

			while (true) {
				if (System.currentTimeMillis() - startTime > runDurationMillis) {
					System.out.println("Stopping after running for 1 minute.");
					break;
				}

				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

				for (ConsumerRecord<String, String> record : records) {
					try {
						String decryptedJson = decryptAndUnwrap(record.value());
						JsonObject payload = JsonParser.parseString(decryptedJson).getAsJsonObject();

						if (isValidStationMessage(payload)) {
							postToCentralStation(decryptedJson);
						} else {
							System.err.println("Invalid message structure, skipping: " + decryptedJson);
						}
					} catch (Exception e) {
						System.err.println("Error processing message: " + e.getMessage());
						e.printStackTrace();
					}
				}
			}


		};
	}
}
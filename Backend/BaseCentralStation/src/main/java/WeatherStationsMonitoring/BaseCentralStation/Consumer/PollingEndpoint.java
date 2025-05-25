package WeatherStationsMonitoring.BaseCentralStation.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PollingEndpoint implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final RestTemplate restTemplate = new RestTemplate();
    private final String topic = "weather-topic";

    public PollingEndpoint() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "weather-monitoring-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "earliest");  // Optional: read from beginning if no offset
//        props.put("enable.auto.commit", "true");     // Optional: auto commit offsets

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String headerJson = KafkaMessageUnwrapper.unwrapHeader(record.value());

                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<String> request = new HttpEntity<>(headerJson, headers);

                    restTemplate.postForEntity("http://localhost:8080/weatherMonitoring/BaseCentralStation", request, String.class);
                    System.out.println("Posted header: " + headerJson);
                } catch (Exception e) {
                    System.err.println("Failed to send POST request: " + e.getMessage());
                }
            }
        }
    }
}

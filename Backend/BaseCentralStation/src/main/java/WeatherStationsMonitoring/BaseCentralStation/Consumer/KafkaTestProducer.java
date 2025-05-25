package WeatherStationsMonitoring.BaseCentralStation.Consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaTestProducer {

    private static final String TOPIC = "weather-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();

        // Required Kafka producer configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Optional tuning
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
//        props.put(ProducerConfig.RETRIES_CONFIG, 3);
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            ObjectMapper mapper = new ObjectMapper();

            ObjectNode weather = mapper.createObjectNode();
            weather.put("humidity", 35);
            weather.put("temperature", 100);
            weather.put("wind_speed", 13);

            ObjectNode header = mapper.createObjectNode();
            header.put("station_id", 1L);
            header.put("s_no", 1L);
            header.put("battery_status", "low");
            header.put("status_timestamp", 1681521224L);
            header.set("weather", weather);

            ObjectNode root = mapper.createObjectNode();
            root.set("header", header);
            root.put("encrypted_payload", "fakeEncryptedDataHere");

            String message = mapper.writeValueAsString(root);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, message);
            Future<RecordMetadata> future = producer.send(record);

            RecordMetadata metadata = future.get();
            System.out.println("Message sent to topic " + metadata.topic() +
                    " partition " + metadata.partition() +
                    " offset " + metadata.offset());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}

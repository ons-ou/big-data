package kafka.producers;

import kafka.sources.GenreAPI;
import kafka.sources.KeywordsAPI;
import kafka.sources.MovieAPI;
import kafka.sources.ReviewAPI;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {
        private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

        public static String getMessage(String topic) throws IOException {
            switch (ProducerTypes.valueOf(topic)){
                case MOVIE:
                    return new MovieAPI().getResponse().toString();
                case GENRES:
                    return new GenreAPI().getResponse().toString();
                case REVIEW:
                    return new ReviewAPI().getResponse().toString();
                case KEYWORDS:
                    return new KeywordsAPI().getResponse().toString();
            }

            return null;
        }

        public static void main(String[] args) {
            String topic = args[0];

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);


            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            executor.scheduleWithFixedDelay(() -> {
                try {
                    String message = getMessage(topic);
                    System.out.println(message);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
                    producer.send(record);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 0, 2, TimeUnit.SECONDS);

            // Shutdown hook to clean up resources
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                executor.shutdown();
                producer.close();
            }));
        }
}

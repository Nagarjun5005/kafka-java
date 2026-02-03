package io.nagarjun.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Demonstrates an asynchronous Kafka Producer with Callback.
 *
 * This example:
 *  - Sends multiple messages to a Kafka topic
 *  - Uses batching for performance
 *  - Logs partition, offset, and timestamp for each message
 *  - Shows how callbacks confirm delivery success or failure
 */
public class ProducerDemoCallBack {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemoCallBack.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting Kafka Producer with Callback");

        // -----------------------------
        // Producer configuration
        // -----------------------------
        Properties properties = new Properties();

        // Bootstrap broker (initial contact point)
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Serializers convert String -> byte[]
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Batch size controls how many bytes are grouped before sending
        // Smaller value here to easily observe batching behavior
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");

        // -----------------------------
        // Create Kafka producer
        // -----------------------------
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        // Producer record (no key -> default partitioner)
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello kafka");

        // -----------------------------
        // Send messages asynchronously
        // -----------------------------
        for (int batch = 0; batch < 10; batch++) {
            for (int msg = 0; msg < 30; msg++) {

                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {

                    // Callback is executed after broker acknowledgement
                    if (exception == null) {
                        log.info("Message sent successfully");
                        log.info("Topic      : {}", metadata.topic());
                        log.info("Partition  : {}", metadata.partition());
                        log.info("Offset     : {}", metadata.offset());
                        log.info("Timestamp  : {}", metadata.timestamp());
                    } else {
                        log.error("Error while producing message", exception);
                    }
                });
            }
        }

        // Give sender thread time to complete async operations
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Ensure all messages are sent before shutting down
        producer.flush();
        producer.close();

        log.info("Kafka Producer closed gracefully");
    }
}

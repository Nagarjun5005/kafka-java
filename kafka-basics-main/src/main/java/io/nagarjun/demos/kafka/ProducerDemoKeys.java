package io.nagarjun.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Producer example demonstrating the use of MESSAGE KEYS.
 *
 * Key concepts shown in this example:
 * 1. How Kafka keys affect partition selection
 * 2. How the default partitioner guarantees ordering per key
 * 3. How callbacks confirm successful delivery
 *
 * IMPORTANT:
 * - Messages with the same key always go to the SAME partition
 * - Ordering is guaranteed only within a partition
 */
public class ProducerDemoKeys {

    /**
     * SLF4J logger for printing producer execution details.
     * Used to log partition assignment for each key.
     */
    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting Kafka Producer with Keys");

        // ----------------------------------------------------
        // Producer Configuration
        // ----------------------------------------------------
        Properties properties = new Properties();

        /**
         * bootstrap.servers:
         * Initial contact point for the Kafka cluster.
         * The producer fetches metadata (brokers, partitions) from here.
         */
        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        /**
         * Key serializer:
         * Converts the message key (String) into byte[] before sending.
         */
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        /**
         * Value serializer:
         * Converts the message value (String) into byte[] before sending.
         */
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        // ----------------------------------------------------
        // Create Kafka Producer
        // ----------------------------------------------------
        /**
         * KafkaProducer is thread-safe and manages:
         * - Network connections
         * - Background sender thread
         * - Record batching and retries
         */
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        // ----------------------------------------------------
        // Sending messages with KEYS
        // ----------------------------------------------------
        /**
         * Outer loop represents multiple batches.
         * Inner loop sends messages with the same set of keys repeatedly.
         *
         * Purpose:
         * - To demonstrate that the SAME key always maps
         *   to the SAME partition across multiple sends.
         */
        for (int batch = 0; batch < 2; batch++) {

            for (int msg = 0; msg < 10; msg++) {

                // Topic name
                String topic = "demo_java";

                /**
                 * Message key.
                 * Same key value guarantees same partition.
                 *
                 * Example keys:
                 * id_0, id_1, id_2, ..., id_9
                 */
                String key = "id_" + msg;

                // Message value
                String value = "hello kafka " + msg;

                /**
                 * ProducerRecord with key:
                 * Kafka uses the key to calculate the partition:
                 *
                 * partition = hash(key) % number_of_partitions
                 */
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                /**
                 * Asynchronous send.
                 * The record is placed in the buffer and sent by
                 * the background sender thread.
                 *
                 * Callback is executed after broker acknowledgment.
                 */
                producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {

                    // Callback success path
                    if (exception == null) {

                        /**
                         * Logging partition assignment proves:
                         * - Same key -> same partition
                         * - Kafka ordering guarantee per key
                         */
                        log.info(
                                "Key: {} | Partition: {} | Offset: {}",
                                key,
                                metadata.partition(),
                                metadata.offset()
                        );

                    } else {
                        // Callback failure path
                        log.error("Error while producing message", exception);
                    }
                });
            }

            /**
             * Small delay to:
             * - Allow async sends to complete
             * - Make logs easier to read
             */
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // ----------------------------------------------------
        // Graceful shutdown
        // ----------------------------------------------------
        /**
         * Flush ensures all buffered records are sent
         * before closing the producer.
         */
        producer.flush();

        /**
         * Close releases network resources and stops
         * the background sender thread.
         */
        producer.close();

        log.info("Kafka Producer closed gracefully");
    }
}

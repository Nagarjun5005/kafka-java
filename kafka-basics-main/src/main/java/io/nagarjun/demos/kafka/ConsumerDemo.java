package io.nagarjun.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Kafka Consumer example.
 *
 * This class demonstrates how to:
 *  - Configure a Kafka consumer
 *  - Subscribe to a topic
 *  - Continuously poll for messages
 *  - Read key, value, partition, and offset information
 *
 * IMPORTANT CONCEPTS:
 *  - Consumers belong to a consumer group
 *  - Each partition is consumed by only one consumer per group
 *  - Ordering is guaranteed per partition
 */
public class ConsumerDemo {

    /**
     * SLF4J logger used to print consumer activity and received records.
     */
    private static final Logger log =
            LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Starting Kafka Consumer");

        /**
         * Consumer group ID.
         *
         * All consumers with the same group ID share the work of reading
         * partitions from the topic.
         */
        String groupId = "my-java-application";

        /**
         * Topic name to subscribe to.
         */
        String topic = "demo_java";

        // ------------------------------------------------
        // Consumer Configuration
        // ------------------------------------------------
        Properties properties = new Properties();

        /**
         * bootstrap.servers:
         * Initial contact point for the Kafka cluster.
         * Used to fetch metadata such as brokers and partitions.
         */
        properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        /**
         * Key deserializer:
         * Converts message keys from byte[] to String.
         * MUST match the producer's key serializer.
         */
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        /**
         * Value deserializer:
         * Converts message values from byte[] to String.
         * MUST match the producer's value serializer.
         */
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        /**
         * group.id:
         * Identifies the consumer group this consumer belongs to.
         * Kafka tracks offsets per partition per group.
         */
        properties.setProperty(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        /**
         * auto.offset.reset:
         * Behavior when no committed offset is found for this group.
         *
         * earliest - start reading from the beginning of the partition
         * latest   - start reading from new messages only
         */
        properties.setProperty(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // ------------------------------------------------
        // Create Kafka Consumer
        // ------------------------------------------------
        /**
         * KafkaConsumer manages:
         *  - Partition assignment
         *  - Fetching records from brokers
         *  - Heartbeats to the group coordinator
         */
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);

        /**
         * Subscribe to the topic.
         * Kafka will automatically assign partitions to this consumer.
         */
        consumer.subscribe(List.of(topic));

        // ------------------------------------------------
        // Poll Loop (core of the consumer)
        // ------------------------------------------------
        while (true) {

            /**
             * poll():
             *  - Fetches data from Kafka
             *  - Sends heartbeats to keep the consumer alive
             *  - Triggers rebalances if needed
             */
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            /**
             * Iterate over received records.
             * Records may come from multiple partitions.
             */
            for (ConsumerRecord<String, String> record : records) {

                /**
                 * Log the message key and value.
                 */
                log.info(
                        "Key: {} | Value: {}",
                        record.key(),
                        record.value()
                );

                /**
                 * Log partition and offset.
                 *
                 * Offset uniquely identifies the position of the record
                 * within the partition.
                 */
                log.info(
                        "Partition: {} | Offset: {}",
                        record.partition(),
                        record.offset()
                );
            }
        }
    }
}

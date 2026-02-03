package io.nagarjun.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBack {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemoCallBack.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Hello Kafka!!");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello kafka");

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if (e == null) {
                    log.info("Message sent successfully ");
                    log.info("Topic: {}", recordMetadata.topic());
                    log.info("Partition: {}", recordMetadata.partition());
                    log.info("Offset: {}", recordMetadata.offset());
                    log.info("Timestamp: {}", recordMetadata.timestamp());
                } else {
                    log.error("Error while producing message", e);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}

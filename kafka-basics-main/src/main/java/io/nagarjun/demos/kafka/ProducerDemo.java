package io.nagarjun.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log =
            LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Hello Kafka!!");

        // create producer properties
        Properties properties=new Properties();
        //properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String>producer=new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String,String>producerRecord=new ProducerRecord<>("demo_java","hello kafka");

        // send data
        producer.send(producerRecord);

        // flush and close the producer
        //tell the producer to send all data abd block until done -- synchronously
        producer.flush();

        producer.close();
    }
}


// kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3
//kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3 --replication-factor 1
//kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning

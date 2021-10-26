package com.rk.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(KafkaProducerDemo.class);
        //create producer properties
        Properties properties = new Properties();

        String bootstrapServers = "127.0.0.1:9092";
        /*properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());*/

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        for (int i = 0; i < 10; i++) {
            //create the producer
            KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hello world" + i);

            //send data - asynchronous
            kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
                //executes everytime a record is successfully sent or an exception is thrown
                if (e == null) {
                    logger.info("Received new meta data: " + "\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition());
                } else
                    logger.error("Error while producing", e);
            });
            //flush data
            kafkaProducer.flush();

            //flush data and close producer
            kafkaProducer.close();
        }
    }
}

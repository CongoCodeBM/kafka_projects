package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo
{
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args)
    {

        log.info("I am a kafka producer");
        //create Producer Properties
        Properties properties = new Properties();

        //connect to Localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        /* the above is equivalent to --bootstrap-server localhost:9092
        when invoking the Kafka CLI commands*/
        //to connect to a secure cluster, you will need to set more properties
        //properties.setProperty("security.protocol", "SASL_SSL"); //HERE ON THE VALUE PART, THE TYPE OF SECURITY PROTOCOL CAN BE CHANGED TO WHATEVER IS THE SECURITY PROTOCOL WE ARE USING
        //properties.setProperty("sasl.jaas.config", "here put the appropriate value"); //put the appropriate key
        //properties.setProperty("sasl.mechanism", "PLAIN"); //put the appropriate key and value

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer Record
        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("demo_java", "hello world");

        //send data -- asynchronous: when you send data to a producer, it is done asynchronously
        producer.send(producerRecord);

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the Producer
        producer.close();

    }
}

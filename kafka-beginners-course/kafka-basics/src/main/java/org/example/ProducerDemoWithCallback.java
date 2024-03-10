package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback
{
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
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

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400"); //usually in production, you would never go for such a smaller batch size of 400, you would keep the default of 16kb

        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); //don't do this in production

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<10; j++)
        {
            for (int i = 0; i < 30; i++)
            {
                //create a Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "hello world " + i);

                //send data -- asynchronous: when you send data to a producer, it is done asynchronously
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n");
                        } else {
                            log.error("Error while producing", e);
                        }

                    }
                });

            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the Producer
        producer.close();

    }
}

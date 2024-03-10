package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown
{
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getSimpleName());
    public static void main(String[] args)
    {

        log.info("I am a kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //create Consumer Properties
        Properties properties = new Properties();

        //connect to Localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        /* the above is equivalent to --bootstrap-server localhost:9092
        when invoking the Kafka CLI commands*/
        //to connect to a secure cluster, you will need to set more properties
        //properties.setProperty("security.protocol", "SASL_SSL"); //HERE ON THE VALUE PART, THE TYPE OF SECURITY PROTOCOL CAN BE CHANGED TO WHATEVER IS THE SECURITY PROTOCOL WE ARE USING
        //properties.setProperty("sasl.jaas.config", "here put the appropriate value"); //put the appropriate key
        //properties.setProperty("sasl.mechanism", "PLAIN"); //put the appropriate key and value

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        //none: if we don't have any existing consumer group we fail. We must set the consumer group before starting the application
        //earliest: read from the beginning of my topic: in the kafka-console-consumer CLI command, it is the --from-beginning part
        //latest: read only new messages
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run(){
                    log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                    consumer.wakeup(); //when we do consumer.poll method call, it is going to throw a wakeup exception

                    //join the main thread to allow the execution of the code in the main thread
                    try
                    {
                        mainThread.join();
                    }
                    catch(InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        );

        try {
            //Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
               // log.info("Polling");

                //create a collection of records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //if there is data to be returned, it will complete right away, it will complete in no time. As soon as the data is received, we move on with the code. if Kafka does not have any dat for us, we are going to wait one second to receive data from Kafka. This then avoids overloading Kafka

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        }
        catch(WakeupException e)
        {
            log.info("Consumer is starting to shut down");
        }
        catch(Exception e)
        {
            log.error("Unexpected exception in the consumer" , e);
        }
        finally
        {
            consumer.close(); //close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shutdown");
        }

    }
}

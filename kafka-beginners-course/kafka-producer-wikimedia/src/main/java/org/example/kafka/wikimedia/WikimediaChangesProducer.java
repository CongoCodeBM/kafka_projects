package org.example.kafka.wikimedia;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer
{
    public static void main(String[] args) throws InterruptedException
    {
        String bootstrapServers = "127.0.0.1:9092";

        //create Producer Properties
        Properties properties = new Properties();

        //connect to Localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        /* the above is equivalent to --bootstrap-server localhost:9092
        when invoking the Kafka CLI commands*/
        //to connect to a secure cluster, you will need to set more properties
        //properties.setProperty("security.protocol", "SASL_SSL"); //HERE ON THE VALUE PART, THE TYPE OF SECURITY PROTOCOL CAN BE CHANGED TO WHATEVER IS THE SECURITY PROTOCOL WE ARE USING
        //properties.setProperty("sasl.jaas.config", "here put the appropriate value"); //put the appropriate key
        //properties.setProperty("sasl.mechanism", "PLAIN"); //put the appropriate key and value

        //set producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //Start the producer in another thread
        eventSource.start();

        // we produce for 10 min and block the program until then
        TimeUnit.MINUTES.sleep(10);
    }
}

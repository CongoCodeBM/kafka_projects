package org.example.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer
{
    public static RestHighLevelClient createOpenSearchClient()
    {
        String connString = "http://localhost:9200"; //using Docker
        //        String connString = "https://... COPY_BONSAI_CREDENTIALS URL HERE"; //using bonsai, go under credentials and copy URL here

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null)
        {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        }
        else
        {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer()
    {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        //create Consumer Properties
        Properties properties = new Properties();

        //connect to Localhost
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // create consumer configs
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //none: if we don't have any existing consumer group we fail. We must set the consumer group before starting the application
        //earliest: read from the beginning of my topic: in the kafka-console-consumer CLI command, it is the --from-beginning part
        //latest: read only new messages
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        //create a consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractId(String json)
    {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //first create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create our Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
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

        // we need to create and execute the index on OpenSearch if it does not exist already
        try(openSearchClient; consumer) //if the try block succeeds or fails, the openSearchClient is going to be closed no matter what
        {
            boolean indexExists =  openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if(!indexExists)
            {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index has been created!");
            }
            else
            {
                log.info("The Wikimedia Index already exists");
            }

            //we subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            //consume data with no shutdown configured
            while(true)
            {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000)); //in case there is no data, we are blocking on this line for 3 seconds
                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : records)
                {
                    //send the record into OpenSearch

                        //Without an id, if we see the same message twice, it will be inserted again into OpenSearch
                             // strategy 1
                             //define an ID using Kafka Record coordinates
                    //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try
                    {
                                //strategy 2 (better)
                                //we extract the ID from the JSON value
                        String id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia") //create a new index request to index some data in OpenSearch
                                .source(record.value(), XContentType.JSON).id(id); //what is the source of our data, and what is the type of data we are sending to OpenSearch

//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT); //send the above index request into OpenSearch
                        bulkRequest.add(indexRequest);

                        //log.info(response.getId());
                    }
                    catch(Exception e)
                    {

                    }
                }

                if(bulkRequest.numberOfActions()>0)
                {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s).");
                    try
                    {
                        Thread.sleep(1000);
                    }
                    catch(InterruptedException e)
                    {
                        e.printStackTrace();
                    }

                    //commit offsets after the batch is consumed
                    consumer.commitSync();
                    log.info("Offsets have been committed!");
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
            openSearchClient.close();
            log.info("The consumer is now gracefully shutdown");
        }
    }
}

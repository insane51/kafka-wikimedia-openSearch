package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;


public class OpenSearchConsumer {

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //Create openSearch Client
        RestHighLevelClient openSearchClient = Helper.createOpenSearchClient();

        // create our Kafka Client
        KafkaConsumer<String, String> consumer = Helper.createKafkaConsumer();

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try(openSearchClient; consumer){
            //we need to create the index on OpenSearch if doesn't exist already

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),RequestOptions.DEFAULT);

            if(!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("#######Index is created");
            }else{
                logger.info("######Index was already exists");
            }

            // we subscribe the consumer
            consumer.subscribe(Collections.singleton(AppConstants.TOPIC));

            while(true){
                ConsumerRecords<String,String> consumerRecord = consumer.poll(Duration.ofMillis(3000));

                logger.info(Integer.toString(consumerRecord.count()));
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String,String> record : consumerRecord){
                    try{
                        // we extract the ID from the JSON value
                        String id = Helper.extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        //To add one by one
                        //IndexResponse indexResponse = openSearchClient.index(indexRequest,RequestOptions.DEFAULT);
                        //logger.info(indexResponse.getId());

                        //For Bulk Request
                        bulkRequest.add(indexRequest);
                    }catch (Exception e){
                        logger.error(e.toString());
                    }
                }
                if(bulkRequest.numberOfActions() >0){
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest,RequestOptions.DEFAULT);
                    logger.info("Inserted " + bulkResponse.getItems().length + " record(s).");
                    try{
                        Thread.sleep(1000);
                    }catch (Exception e){
                        logger.error(e.toString());
                    }
                    // commit offsets after the batch is consumed
                    consumer.commitSync();
                    logger.info("Offsets have been committed!");
                }

            }
        }catch (WakeupException e) {
            logger.info("Consumer is starting to shut down");
        } catch (Exception e) {
            logger.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            openSearchClient.close();
            logger.info("The consumer is now gracefully shut down");
        }
    }
}

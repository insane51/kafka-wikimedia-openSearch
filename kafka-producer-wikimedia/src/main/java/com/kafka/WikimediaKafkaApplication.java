package com.kafka;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class WikimediaKafkaApplication {

	public static Logger logger = LoggerFactory.getLogger(WikimediaKafkaApplication.class.getSimpleName());
	public static void main(String[] args) throws InterruptedException {

		SpringApplication.run(WikimediaKafkaApplication.class, args);
		logger.info("####Main method runs");


		Properties props = new Properties();
		props.put("bootstrap.servers", "dear-anchovy-9404-eu1-kafka.upstash.io:9092");
		props.put("sasl.mechanism", "SCRAM-SHA-256");
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZGVhci1hbmNob3Z5LTk0MDQk2INMjpEdRAmSz_xdaVSkqE79wB7I1BQRYark-Q8\" password=\"NzkyNzI4ZjMtZmVmNS00YzU2LWI4ZjgtMmM0YzgyOGE2NWQx\";");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		try{
			EventHandler eventHandler = new WikimediaChangeHandler(producer, com.kafka.producer.AppConstants.TOPIC_ONLINE);
			EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(com.kafka.producer.AppConstants.PRODUCER_URL));
			EventSource eventSource = builder.build();

			//Start the producer in another thread
			eventSource.start();

		}catch(Exception e){
			logger.error(e.toString());
		}

		//we produce for 1-minute and block the program until then
		TimeUnit.SECONDS.sleep(10);
	}
}

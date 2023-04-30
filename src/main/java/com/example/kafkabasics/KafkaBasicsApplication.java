package com.example.kafkabasics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaBasicsApplication {
	
	private static final Logger log=LoggerFactory.getLogger(KafkaBasicsApplication.class);

	public static void main(String[] args) {
		//SpringApplication.run(KafkaBasicsApplication.class, args);
		log.info("Success");
		
		//create properties
		//producer properties
		// create producer 
		//send data
		//flush
		//close
		
		Properties properties=new Properties();
		
		//connect to conduktor playground
		properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol","SASL_SSL");
		properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"6KwEakLogOnQ5HfdGjufeT\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2S3dFYWtMb2dPblE1SGZkR2p1ZmVUIiwib3JnYW5pemF0aW9uSWQiOjcyNzkwLCJ1c2VySWQiOjg0NTg2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhOWUyMWIyNC1kNThhLTRkNDQtODM5Mi05MTVmNzA3ZWYyNWYifX0.U1Gvk85kaZeh6GoZ0eXZzlQAYhbrafNGaPlgUGMfTO8\";");
		properties.setProperty("sasl.mechanism","PLAIN");
		
		//set properties
		properties.setProperty("key.serializer", StringSerializer.class.getName() );
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		
		//create producer
		KafkaProducer<String,String> producer=new KafkaProducer<>(properties);
		
		for(int j=0 ; j<2;j++) {
			//sticky partitioner
			for(int i=0;i<20;i++) {
				
				String topic="success-topic";
				String value="Near Success Message ";
				String key="key " + i; 
				//producer record
				ProducerRecord<String,String> record=new ProducerRecord<>(topic,key,value);
				
				producer.send(record,new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if(e ==null) {
						log.info("\n key ->" + key +					
						"\n partition " + metadata.partition() 
								);
						}else {
							log.error("Error while producing "+ e);
						}				
					}
				});
							
			}
			
		}
		
		
		
		
		
		producer.flush();
		producer.close();
		
		
		
	}

}

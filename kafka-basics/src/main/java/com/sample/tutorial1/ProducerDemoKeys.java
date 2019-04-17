package com.sample.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        //Create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i  = 0; i < 10; i++) {
            //create a producer record

            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key: " + key);

            //id 0 in partition 1
            //id 1 in partition 0
            //id 2 in partition 2
            //id 3 in partition 0
            //id 4 in partition 2
            //id 5 in partition 2
            //id 6 in partition 0
            //id 7 in partition 2
            //id 8 in partition 1
            //id 9 in partition 2

            //send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes every time a record is successfully sent or an excpetion is thrown
                    if (exception == null) {
                        //the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offsets :" + metadata.offset() + "\n" +
                                "TimeStamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); //block the .send() to make it  synchronous - do not so this in partition!
        }

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();




    }
}

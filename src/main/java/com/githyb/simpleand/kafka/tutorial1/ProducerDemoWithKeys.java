package com.githyb.simpleand.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        final String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i<10; i++){

            final String topic = "first_topic";
            final String value = "hello world!1" + Integer.toString(i);
            final String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            
            //quando é fornecida uma key, a mensagem com uma mesma key é sempre gravada na mesma partição.
            logger.info("Key: " + key);
            
            producer.send(record, new Callback() {

                public void onCompletion(final RecordMetadata recordMetadata, final Exception e) {
                    if (e == null) {
                        //record successfully
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        e.printStackTrace();
                    }
                }
            }).get();  //para tonar o bloco .send sincrono, NAO FAZER ISSO EM PROD!!!! Por causa de performance.


        }

        //só por garantia, mas o close já faz isso implicitamente
        producer.flush();

        producer.close();
    }

}

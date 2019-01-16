package com.github.simpleand.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

    enum AutoOffsetResetConfigEnum{
        EARLIEST("earliest"),
        LATEST("latest"),
        NONE("none");

        private String value;

        AutoOffsetResetConfigEnum(String value){
            this.value = value;
        }

        public String getValue(){
            return value;
        }
    }


    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        final String bootstrapServers = "127.0.0.1:9092";
        final String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetConfigEnum.EARLIEST.getValue());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assing and seek é mais utilizando para reproduzir dados ou buscar uma mensagem especifica
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        long offsetToReadFrom = 15L;
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar = 0;
        boolean keepOnReading = true;

        while (keepOnReading){
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar +=1;
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }

        logger.info("Exiting the application");
    }

}

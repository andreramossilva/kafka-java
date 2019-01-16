package com.github.simpleand.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {

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
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

        final String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my_fifth_application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetConfigEnum.EARLIEST.getValue());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("first_topic"));

        while (true){
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

}

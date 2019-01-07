package com.githyb.simpleand.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

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
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread(){

    }

    private void run(){
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        final String bootstrapServers = "127.0.0.1:9092";
        final String topic = "first_topic";
        final String groupId = "my_sixth_application";

        logger.info("Creating the consumer thread");
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(groupId,topic,bootstrapServers,latch);
        
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()-> {

                logger.info("Caught shutdown hook");
                ((ConsumerRunnable) myConsumerRunnable).shutDown();

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                logger.info("Application has exited");
            }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        }   finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{

        final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private KafkaConsumer<String, String> consumer;

        private String topic;
        private CountDownLatch latch;
        private String groupId;
        private String bootstrapServers;

        public ConsumerRunnable( String groupId, String topic, String bootstrapServers, CountDownLatch latch){
            this.topic = topic;
            this.groupId = groupId;
            this.bootstrapServers = bootstrapServers;
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetConfigEnum.EARLIEST.getValue());

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {

            try {
                while (true) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("Received shutdown signal!");
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown(){
            //interrompe o consumer.poll()
            //levantara uma WakeUpException
            consumer.wakeup();
        }
    }

}

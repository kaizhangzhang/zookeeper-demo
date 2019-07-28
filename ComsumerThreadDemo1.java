package com.demo.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ComsumerThreadDemo1 {
    public static final String brokerList = "localhost:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group-demo";
    public static void main(String... args) {
        Properties properties = initConfig();
        KafkaConsumerThread kafkaConsumerThread = new KafkaConsumerThread(properties, topic);
        kafkaConsumerThread.start();
    }
    public static Properties initConfig() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", brokerList);
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("group.id", groupId);
        return kafkaProps;

    }
    public static class KafkaConsumerThread extends Thread{
        private ExecutorService executorService;
        private KafkaConsumer<String, String> consumer;
        public KafkaConsumerThread(Properties properties, String topic) {
            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singletonList(topic));
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            System.out.println(partitionInfos.size());
            executorService = new ThreadPoolExecutor(partitionInfos.size(), partitionInfos.size(), 100, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());

        }
        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                    if (!consumerRecords.isEmpty()) {
                        executorService.execute(new ConsumerHandlerRunable(consumerRecords));
                    }
                }
            }catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                executorService.shutdown();
            }
        }
    }
    public static class ConsumerHandlerRunable implements Runnable{
        private final ConsumerRecords<String, String> consumerRecords;
        public ConsumerHandlerRunable(ConsumerRecords<String, String> consumerRecords) {
            this.consumerRecords = consumerRecords;
        }
        @Override
        public void run() {
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println("thread = " + Thread.currentThread().getName() + ", topic = " + record.topic() +
                        ", partition = " + record.partition() +
                        " , offset = " + record.offset() +
                        ", value = " + record.value());
            }
        }
    }
}

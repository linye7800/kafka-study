package com.lincoln.lin.kafkastudy.consumer;

import java.security.Key;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月24日 10:53 上午
 */
public class ConsumerSample {

    public final static String TOPIC_NAME = "lincoln-topic";
    public final static String TOPIC_NAME_VINA = "vina-topic";

    public static void main(String[] args) {
        demoHelloWord();
//        demoHelloWord2();
        // 手动提交offset
//        commitOffset();

        // 手动对每个partition进行提交
//        commitOffsetWithPartition();

        // 手动订阅某个或某些分区
//        commitOffsetWithPartition2();
    }

    /**
     * 手动提交offset
     */
    private static void demoHelloWord() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.117.4:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);
        // 消费订阅哪一个或哪几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME, TOPIC_NAME_VINA));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    private static void demoHelloWord2() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.117.4:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);
        // 消费订阅哪一个或哪几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME_VINA));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("------> partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 提交offset
     */
    private static void commitOffset() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.117.4:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);
        // 消费订阅哪一个或哪几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                // 想把数据保存到数据库，成功就成功，不成功...
                // TODO record 2 db
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                // 如果失败，则回滚，不要提交offset
            }
            // 如果成功，手动通知offset提交
            // 如果失败就不要提交offset，成功了就提交，失败了不提交可以达到再去重新消费一次的目的。
            consumer.commitAsync();
        }
    }

    /**
     * 手动提交offset，并手动控制partition
     */
    private static void commitOffsetWithPartition() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.117.4:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);
        // 消费订阅哪一个或哪几个Topic
        consumer.subscribe(Arrays.asList(TOPIC_NAME_VINA));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                long lastOffset = pRecord.get(pRecord.size() -1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset+1));
                // 提交offset
                consumer.commitAsync();
                System.out.println("=============== partition - " + partition + " ===============");
            }

        }
    }

    /**
     * 手动提交offset，并手动控制partition（更高级）
     */
    private static void commitOffsetWithPartition2() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.16.117.4:9092");
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer(properties);

        // lincoln-topic-0,1 两个partition
        TopicPartition p0= new TopicPartition(TOPIC_NAME, 0);
        TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

        // 消费订阅哪一个或哪几个Topic
//        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 消费订阅某个topic的某个分区
        consumer.assign(Arrays.asList(p0));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            // 每个partition单独处理
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                for (ConsumerRecord<String, String> record : pRecord) {
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                }
                long lastOffset = pRecord.get(pRecord.size() -1).offset();
                // 单个partition中的offset，并且进行提交
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                offset.put(partition, new OffsetAndMetadata(lastOffset+1));
                // 提交offset
                consumer.commitAsync();
                System.out.println("=============== partition - " + partition + " ===============");
            }
        }
    }



}

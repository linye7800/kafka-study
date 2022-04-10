package com.lincoln.lin.kafkastudy.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerThreadSample {
    private final static String TOPIC_NAME="lincoln-topic";
    public final static String TOPIC_NAME_VINA = "vina-topic";

    /**
     * 这种类型是经典模式，每一个线程都是单独创建一个KafkaConsumer，用于保证线程安全
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerRunner r1 = new KafkaConsumerRunner();
        Thread t1 = new Thread(r1);

        t1.start();

        Thread.sleep(15000);

        r1.shutdown();
    }

    /**
     * Kafka的consumer不是线程安全的，所以我们创建每一个线程的时候，都单独创建一个consumer，所以保证了线程安全。
     */
    public static class KafkaConsumerRunner implements Runnable{
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;

        public KafkaConsumerRunner() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "172.16.117.4:9092");
            props.put("group.id", "test");
            props.put("enable.auto.commit", "false");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);

            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
            TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);
            TopicPartition p2 = new TopicPartition(TOPIC_NAME, 2);

            consumer.assign(Arrays.asList(p0,p1,p2));
        }


        public void run() {
            try {
                while(!closed.get()) {
                    //处理消息
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                        // 处理每个分区的消息
                        for (ConsumerRecord<String, String> record : pRecord) {
                            System.out.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                                    record.partition(),record.offset(), record.key(), record.value());
                        }

                        // 返回去告诉kafka新的offset
                        long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                        // 注意加1
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                    }

                }
            }catch(WakeupException e) {
                if(!closed.get()) {
                    throw e;
                }
            }finally {
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }

}

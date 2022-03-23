package com.lincoln.lin.kafkastudy.wechat.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 10:08 下午
 */
@Configuration
public class KafkaConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    /**
     * 使用@Bean的原因是：依赖spring本身提供的上下文，构建一个单例模式，所有的线程共享一个producer
     * @return
     */
    @Bean
    public Producer kafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());

        properties.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcksConfig());
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.lincoln.lin.kafkastudy.producer.SamplePartition");

        // Producer的主对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
        // 所有的通道打开都需要关闭
    }

}

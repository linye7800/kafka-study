package com.lincoln.lin.kafkastudy.wechat.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 10:08 下午
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "wechat.kafka")
public class KafkaProperties {

    private String bootstrapServers;
    private String acksConfig;
    private String partitionerClass;

}

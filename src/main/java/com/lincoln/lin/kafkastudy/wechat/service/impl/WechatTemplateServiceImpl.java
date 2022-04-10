package com.lincoln.lin.kafkastudy.wechat.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lincoln.lin.kafkastudy.wechat.config.WechatTemplateProperties;
import com.lincoln.lin.kafkastudy.wechat.config.WechatTemplateProperties.WechatTemplate;
import com.lincoln.lin.kafkastudy.wechat.service.WechatTemplateService;
import com.lincoln.lin.kafkastudy.wechat.util.FileUtils;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 3:38 下午
 */
@Slf4j
@Service
public class WechatTemplateServiceImpl implements WechatTemplateService {

    @Autowired
    private WechatTemplateProperties properties;

    @Autowired
    private Producer producer;

    /**
     *  获取目前active为true的调查问卷模板
     * @return
     */
    @Override
    public WechatTemplate getWechatTemplate() {
        List<WechatTemplateProperties.WechatTemplate> templates = properties.getTemplates();
        Optional<WechatTemplate> wechatTemplate = templates.stream().filter(t -> t.isActive()).findFirst();
        return wechatTemplate.isPresent() ? wechatTemplate.get() : null;
    }

    @Override
    public void templateReported(JSONObject reportInfo) {
        // Kafka producer 将数据推送至Kafka Topic
        log.info("templateReported : [{}]", reportInfo);
        String topic_name = "lincoln-topic";
        // 发送kafka数据
        String templateId = reportInfo.getString("templateId");
        JSONArray reportData = reportInfo.getJSONArray("result");

        // 如果template相同，后续在统计分析时，可以考虑将相同的id的内容放入同一个partition，便于分析使用
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(topic_name, templateId,reportData);

        // 核心：1.Kafka的partition是线程安全的，建议多线程复用，如果每个线程都创建，出现大量上下文切换和争抢，影响效率。
        // 核心：2.Kafka Producer的key是一个很重要的内容：2.1 我们可以根据Key完成Partition的负载均衡 2.2 Flink, Spark Streaming之类的实时分析工具做更快速处理
        // 核心：3.ack - all， kafka层面上已经有了一次的消息投递保障，但是如果真不想丢失数据，最好自行处理异常

        // 1. 通过try catch来判断 是否投递成功
        try {
            producer.send(record);
        }catch (Exception e) {
            // 将数据加入重复队列， redis，es等
        }

        // 2. 通过Future 的结果来判断
//        Future future = producer.send(record);
    }

    @Override
    public JSONObject templateStatistics(String templateId) {
        // 判断数据结果获取类型
        if (properties.getTemplateResultType() == 0) {
            // 文件获取
            return FileUtils.readFile2JsonObject(properties.getTemplateFilePathResult()).get();
        } else {
            // DB获取
        }
        return null;
    }
}

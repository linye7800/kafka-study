package com.lincoln.lin.kafkastudy.wechat.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.lincoln.lin.kafkastudy.wechat.config.WechatTemplateProperties;
import com.lincoln.lin.kafkastudy.wechat.config.WechatTemplateProperties.WechatTemplate;
import com.lincoln.lin.kafkastudy.wechat.service.WechatTemplateService;
import com.lincoln.lin.kafkastudy.wechat.util.FileUtils;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
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

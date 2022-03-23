package com.lincoln.lin.kafkastudy.wechat.service;

import com.alibaba.fastjson.JSONObject;
import com.lincoln.lin.kafkastudy.wechat.config.WechatTemplateProperties;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 3:37 下午
 */
public interface WechatTemplateService {


    /**
     * 获取微信调查问卷模板
     * @return
     */
    WechatTemplateProperties.WechatTemplate getWechatTemplate();

    /**
     * 上报调查问卷填写结果
     * @param jsonObject
     */
    void templateReported(JSONObject jsonObject);

    /**
     * 获取调查问卷的统计结果
     * @param templateId
     * @return
     */
    JSONObject templateStatistics(String templateId);

}

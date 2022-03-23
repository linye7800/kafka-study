package com.lincoln.lin.kafkastudy.wechat.controller;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.lincoln.lin.kafkastudy.wechat.common.BaseResponseVO;
import com.lincoln.lin.kafkastudy.wechat.config.WechatTemplateProperties;
import com.lincoln.lin.kafkastudy.wechat.service.WechatTemplateService;
import com.lincoln.lin.kafkastudy.wechat.util.FileUtils;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 3:36 下午
 */
@Controller
@RequestMapping(value = "/v1")
public class WechatTemplateController {

    @Autowired
    private WechatTemplateProperties properties;

    @Autowired
    private WechatTemplateService wechatTemplateService;

    @RequestMapping(value = "/template", method = RequestMethod.GET)
    @ResponseBody
    public BaseResponseVO getTemplate() {

        WechatTemplateProperties.WechatTemplate wechatTemplate = wechatTemplateService.getWechatTemplate();
        Map<String, Object> result = Maps.newHashMap();
        result.put("templatedId", wechatTemplate.getTemplateId());
        result.put("template", FileUtils.readFile2JsonArray(wechatTemplate.getTemplateFilePath()));

        return BaseResponseVO.success(result);
    }

    @RequestMapping(value = "/template/result", method = RequestMethod.GET)
    @ResponseBody
    public BaseResponseVO templateStatistics(@RequestParam(value = "templateId", required = false)String templateId) {

        JSONObject statics = wechatTemplateService.templateStatistics(templateId);

        return BaseResponseVO.success(statics);
    }

    @RequestMapping(value = "/template/report", method = RequestMethod.POST)
    @ResponseBody
    public BaseResponseVO dataReport(@RequestBody String reportData) {

        wechatTemplateService.templateReported(JSONObject.parseObject(reportData));

        return BaseResponseVO.success();
    }

}

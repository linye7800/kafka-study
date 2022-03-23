package com.lincoln.lin.kafkastudy.wechat.config;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * description:
 *
 * @author linye
 * @date 2022年03月23日 3:49 下午
 */
@Configuration
@ConfigurationProperties(prefix = "template")
@Data
public class WechatTemplateProperties {

    private List<WechatTemplate> templates;
    /**
     * 0-文件获取 1-数据库获取 2-es获取
     */
    private int templateResultType;
    private String templateFilePathResult;

    @Data
    public static class WechatTemplate{
        // 模板编号
        private String templateId;
        private String templateFilePath;
        private boolean active;
    }

}

package com.github.nowackia.kafka.connect.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Config for hdfs sinking connector
 */
public class HdfsSinkConnectorConfig extends AbstractConfig {

    public static final String TASK_CLASS = "task.class";
    private static final String TASK_CLASS_DOC = "The connector task class.";

    public HdfsSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public HdfsSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TASK_CLASS, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, TASK_CLASS_DOC);
    }

    public String getTaskClass(){
        return this.getString(TASK_CLASS);
    }

}
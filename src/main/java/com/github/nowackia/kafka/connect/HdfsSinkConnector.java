package com.github.nowackia.kafka.connect;

import com.github.nowackia.kafka.connect.config.HdfsSinkConnectorConfig;
import com.github.nowackia.kafka.connect.util.Version;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Task with the sink logic (saving to both local drive and hdfs)
 */
public class HdfsSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(HdfsSinkConnector.class);
    private Map<String, String> configProperties;
    private HdfsSinkConnectorConfig config;

    @Override
    public String version() {
        return Version.CURRENT.toString();
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            configProperties = props;
            config = new HdfsSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start HdfsSinkConnector due to configuration error", e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        Class<? extends Task> clazz = null;
        try {
            clazz = Class.forName(config.getTaskClass()).asSubclass(HdfsFileSinkTask.class);
        } catch (ClassNotFoundException e) {
            log.error("Couldn't instantiate task, {}", e);
        }
        return clazz;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>();
        taskProps.putAll(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }

        return taskConfigs;
    }

    @Override
    public void stop() throws ConnectException {

    }

    @Override
    public ConfigDef config() {
        return HdfsSinkConnectorConfig.conf();
    }
}
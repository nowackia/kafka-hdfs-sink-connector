package com.github.nowackia.kafka.connect.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Config for hdfs sinking task
 */
public class HdfsFileSinkTaskConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileSinkTaskConfig.class);

    public static final String NAME = "name";
    private static final String NAME_DOC = "Connector name.";

    public static final String NOTIFICATION_ENABLED = "notification.enabled";
    private static final String NOTIFICATION_ENABLED_DOC = "Enable sending notifications at the end of file processing.";

    public static final String HDFS_COMPRESSION_ENABLED = "hdfs.compression.enabled";
    private static final String HDFS_COMPRESSION_ENABLED_DOC = "Enable compressing to gzip format for sinked files.";

    public static final String HDFS_CONFIG_LOCATION = "hdfs.config.location";
    private static final String HDFS_CONFIG_LOCATION_DOC = "HDFS config files location.";

    public static final String HDFS_LOCATION_FS = "hdfs.fs.location";
    private static final String HDFS_LOCATION_FS_DOC = "Hadoop HDFS server location.";

    public static final String HDFS_LOCATION_POLICY = "hdfs.policy.location";
    private static final String HDFS_LOCATION_POLICY_DOC = "Location to sink to for files.";

    public static final String HDFS_FILE_REGEX = "hdfs.policy.files";
    private static final String HDFS_FILE_REGEX_DOC = "Regex to match the location by file name.";

    public static final String HDFS_NOTIFICATION_REGEX = "hdfs.policy.notification";
    private static final String HDFS_NOTIFICATION_REGEX_DOC = "Topic to send notification to for specific file(s).";

    public static final String HDFS_TYPE_REGEX = "hdfs.policy.type";
    private static final String HDFS_TYPE_REGEX_DOC = "Type of file to sink.";

    public static final String HDFS_PRINCIPAL = "hdfs.principal";
    private static final String HDFS_PRINCIPAL_DOC = "Principal location for kerberos authentication.";

    public static final String HDFS_KEYTAB = "hdfs.keytab";
    private static final String HDFS_KEYTAB_DOC = "Keytab location for kerberos authentication.";

    public static final String LOG_PERCENTAGE_INTERVAL = "logging.percentage.interval";
    private static final String LOG_PERCENTAGE_INTERVAL_DOC = "Interval in percentage [0 ... 100%] at which logging file processing progress happens.";

    public static final String KERBEROS_REFRESH = "kerberos.refresh";
    private static final String KERBEROS_REFRESH_DOC = "Time in hours between kerberos principle refresh.";

    private Map<String, HdfsPolicyFileConfig> fileLocations = new HashMap<>();

    public HdfsFileSinkTaskConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);

        for(Map.Entry<String, String> entry : parsedConfig.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if(key.contains(HDFS_FILE_REGEX)) {
                String id = key.replace(HDFS_FILE_REGEX, "");
                for(String file : value.split(",")) {
                    file = file.trim();
                    String notification = parsedConfig.get(HDFS_NOTIFICATION_REGEX + id);
                    String type = parsedConfig.get(HDFS_TYPE_REGEX + id);
                    if(type != null) {
                        fileLocations.put(file, new HdfsPolicyFileConfig(type, notification));
                    } else {
                        log.error("Missing required propertie(s) (" + HDFS_TYPE_REGEX + ") for " + key);
                    }
                }
            }
        }
    }

    public HdfsFileSinkTaskConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, NAME_DOC)
                .define(NOTIFICATION_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, NOTIFICATION_ENABLED_DOC)
                .define(HDFS_COMPRESSION_ENABLED, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, HDFS_COMPRESSION_ENABLED_DOC)
                .define(HDFS_CONFIG_LOCATION, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, HDFS_CONFIG_LOCATION_DOC)
                .define(HDFS_LOCATION_FS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HDFS_LOCATION_FS_DOC)
                .define(HDFS_LOCATION_POLICY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HDFS_LOCATION_POLICY_DOC)
                .define(LOG_PERCENTAGE_INTERVAL, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, LOG_PERCENTAGE_INTERVAL_DOC)
                .define(HDFS_FILE_REGEX + ".other", ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_FILE_REGEX_DOC)
                .define(HDFS_NOTIFICATION_REGEX + ".other", ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_NOTIFICATION_REGEX_DOC)
                .define(HDFS_TYPE_REGEX + ".other", ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_TYPE_REGEX_DOC)
                .define(HDFS_PRINCIPAL, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_PRINCIPAL_DOC)
                .define(HDFS_KEYTAB , ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, HDFS_KEYTAB_DOC)
                .define(KERBEROS_REFRESH , ConfigDef.Type.INT, 1, ConfigDef.Importance.LOW, KERBEROS_REFRESH_DOC);
    }

    public String getName() {
        return this.getString(NAME);
    }

    public int getLogPercentageInterval() {
        return this.getInt(LOG_PERCENTAGE_INTERVAL);
    }

    public boolean isNotificationEnabled() {
        return this.getBoolean(NOTIFICATION_ENABLED);
    }

    public String getHdfsFs() {
        return this.getString(HDFS_LOCATION_FS);
    }

    public boolean isCompressionEnabled() {
        return this.getBoolean(HDFS_COMPRESSION_ENABLED);
    }

    public String getHdfsConfigLocation() {
        return this.getString(HDFS_CONFIG_LOCATION);
    }

    public String getHdfsLocation() {
        return this.getString(HDFS_LOCATION_POLICY);
    }

    public String getHdfsPrincipal() {
        return this.getString(HDFS_PRINCIPAL);
    }

    public String getHdfsKeytab() {
        return this.getString(HDFS_KEYTAB);
    }

    public int getKerberosRefresh() {
        return this.getInt(KERBEROS_REFRESH);
    }

    public Map<String, HdfsPolicyFileConfig> getFileLocations() {
        return this.fileLocations;
    }
}
package com.github.nowackia.kafka.connect;

import com.github.nowackia.kafka.connect.config.HdfsFileSinkTaskConfig;
import com.github.nowackia.kafka.connect.config.HdfsPolicyFileConfig;
import com.github.nowackia.kafka.connect.hdfs.writer.BinaryFileWriter;
import com.github.nowackia.kafka.connect.hdfs.writer.CsvFileWriter;
import com.github.nowackia.kafka.connect.hdfs.writer.FileWriter;
import com.github.nowackia.kafka.connect.hdfs.writer.TextFileWriter;
import com.github.nowackia.kafka.connect.notifications.ImmutableOrchestrationNotification;
import com.github.nowackia.kafka.connect.notifications.MarketsNotificationProducer;
import com.github.nowackia.kafka.connect.notifications.OrchestrationNotification;
import com.github.nowackia.kafka.connect.util.Version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Connector for sinking (saving to both local drive and hdfs)
 */
public class HdfsFileSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileSinkTask.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd_HH-mm-ss_SSS");

    private HdfsFileSinkTaskConfig config;
    private FileSystem fs;
    private MarketsNotificationProducer producer;
    private Map<String, FileWriter> writers;
    private Map<String, HdfsPolicyFileConfig> configs;

    private UserGroupInformation ugi;
    private boolean isKerberosEnabled = false;

    public HdfsFileSinkTask() {
    }

    @Override
    public String version() {
        return Version.CURRENT.toString();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            config = new HdfsFileSinkTaskConfig(props);

            Configuration fsConfig = new Configuration();
            if(config.getHdfsConfigLocation() != null) {
                log.info("Including hdfs configuration from: {}", config.getHdfsConfigLocation());
                fsConfig.addResource(new Path(config.getHdfsConfigLocation() + "/core-site.xml"));
                fsConfig.addResource(new Path(config.getHdfsConfigLocation() + "/hdfs-site.xml"));
            }
            fsConfig.set("fs.defaultFS", config.getHdfsFs());
            fsConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            fsConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            isKerberosEnabled = config.getHdfsPrincipal() != null && config.getHdfsKeytab() != null;
            if(isKerberosEnabled) {
                fsConfig.set("hadoop.security.authentication", "kerberos");
                fsConfig.set("hadoop.security.authorization", "true");

                UserGroupInformation.setConfiguration(fsConfig);
                UserGroupInformation.loginUserFromKeytab(config.getHdfsPrincipal(), config.getHdfsKeytab());
                ugi = UserGroupInformation.getLoginUser();
                log.info("Authenticated as: " + ugi.getUserName());

                ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
                service.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            log.info("Re-authenticating for user: {}", ugi.getUserName());
                            ugi.reloginFromKeytab();
                        } catch (IOException e) {
                            log.error("Error in refreshing authentication", e);
                        }
                    }
                }, config.getKerberosRefresh(), config.getKerberosRefresh(), TimeUnit.HOURS);
            }

            fs = FileSystem.get(fsConfig);

            writers = new HashMap<>();

            if(config.isNotificationEnabled()) {
                producer = new MarketsNotificationProducer(props, isKerberosEnabled);
            } else
                producer = null;

            configs = new HashMap<>();
        } catch (Exception e) {
            log.error("Couldn't start HdfsFileSinkTask:", e);
        }
    }

    /**
     *
     * Method used to sinking records
     * @param records records received from kafka
     */
    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        try {
            for (SinkRecord record : records) {
                if (record.key() != null && record.value() != null) {
                    try {
                        if(record.key() instanceof Struct) {
                            Struct keyStruct = (Struct) record.key();
                            if (validate(keyStruct.schema())) {
                                String source = keyStruct.getString("source");
                                String sourceFileName = keyStruct.getString("file-name");
                                long totalCount = keyStruct.getInt64("file-total-count");
                                String cobDate = keyStruct.getString("cob-date");
                                boolean isFirst = keyStruct.getBoolean("first-field");
                                boolean isLast = keyStruct.getBoolean("last-field");
                                String fileType = keyStruct.getString("file-type");

                                if(record.value() instanceof ArrayList) {
                                    ArrayList<Struct> valueStructs = (ArrayList<Struct>) record.value();

                                    FileWriter writer = getWriter(sourceFileName);
                                    if (writer == null) {
                                        HdfsPolicyFileConfig fileConfig = config(sourceFileName, cobDate, source, config.getFileLocations());
                                        if (fileConfig != null) {
                                            this.configs.put(sourceFileName, fileConfig);

                                            if (valueStructs.size() > 0) {
                                                writer = createWriter(fs, fileType, fileConfig.getLocation(), sourceFileName, isFirst, totalCount, valueStructs.get(0).schema(), config.isCompressionEnabled(), isKerberosEnabled);
                                                writers.put(sourceFileName, writer);
                                                log.info("Starting writing new file {}", writer.getFileName());
                                            }
                                        }
                                    }

                                    if (writer != null) {
                                        writer.write(valueStructs);
                                        writer.logProgress(log, config.getLogPercentageInterval());
                                    }

                                    if (isLast && writer != null) {
                                        HdfsPolicyFileConfig fileConfig = this.configs.get(sourceFileName);
                                        if (fileConfig != null) {
                                            log.info("Successfully finished writing file {} to {}", writer.getFileName(), fileConfig.getLocation());
                                            writer.close();
                                            writers.remove(sourceFileName);

                                            notification(fileConfig.getLocation(), writer.getFileName(), cobDate, source, fileConfig.getNotificationTopic());
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Error processing value {}", record.value(), e);
                    }
                } else {
                    log.warn("Received unknown record type: " + record.value());
                }
            }
        } catch (ConnectException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for(FileWriter writer : writers.values())
            writer.flush();
    }

    @Override
    public void stop() throws ConnectException {
        for(FileWriter writer : writers.values())
            writer.close();
    }

    private FileWriter getWriter(String fileName) {
        return writers.get(fileName);
    }

    private FileWriter createWriter(FileSystem fs, String fileType, String location, String fileName, boolean isFirst, long total, Schema valueSchema, boolean isCompressionEnabled) throws IOException {
        return this.createWriter(fs, fileType, location, fileName, isFirst, total, valueSchema, isCompressionEnabled, false);
    }

    private FileWriter createWriter(FileSystem fs, String fileType, String location, String fileName, boolean isFirst, long total, Schema valueSchema, boolean isCompressionEnabled, boolean isKerberosEnabled) throws IOException {
        FileWriter writer = null;
        if(fileType.equalsIgnoreCase("csv"))
            writer = new CsvFileWriter(fs, location, fileName, isFirst, total, valueSchema, isCompressionEnabled);
        else if(fileType.equalsIgnoreCase("text"))
            writer = new TextFileWriter(fs, location, fileName, isFirst, total, valueSchema, isCompressionEnabled);
        else if(fileType.equalsIgnoreCase("binary"))
            writer = new BinaryFileWriter(fs, location, fileName, isFirst, total, valueSchema, false);
        else {
            log.warn("No default FileWriter defined for {}, using BinaryFileWriter", fileType);
            writer = new BinaryFileWriter(fs, location, fileName, isFirst, total, valueSchema, isCompressionEnabled);
        }
        return writer;
    }

    protected boolean validate(Schema schema) {
        return schema.field("source") != null && schema.field("file-name") != null
                && schema.field("first-field") != null && schema.field("last-field") != null
                && schema.field("file-total-count") != null && schema.field("cob-date") != null
                && schema.field("file-type") != null;
    }

    protected HdfsPolicyFileConfig config(String fileName, String cobDate, String source, Map<String, HdfsPolicyFileConfig> fileLocations) {
        Map.Entry<String, HdfsPolicyFileConfig> policyConfig = null;

        for(Map.Entry<String, HdfsPolicyFileConfig> entry : fileLocations.entrySet()) {
            Pattern p = Pattern.compile(entry.getKey());
            Matcher m = p.matcher(fileName);
            if(m.matches()) {
                policyConfig = entry;
                break;
            }
        }

        if(policyConfig != null ) {
            String notificationTopic = policyConfig.getValue().getNotificationTopic();
            String location = this.config.getHdfsLocation().replace("{yyyyMMdd}", cobDate).replace("{source}", source);

            HdfsPolicyFileConfig newConfig = new HdfsPolicyFileConfig(notificationTopic, location);
            return newConfig;
        }

        return null;
    }

    protected void notification(String location, String fileName, String cobDate, String source, String notificationTopic) {
        if(config.isNotificationEnabled() && notificationTopic != null && producer != null) {
            OrchestrationNotification notification = ImmutableOrchestrationNotification.builder()
                    .timestamp(System.currentTimeMillis())
                    .inputPath(location + fileName)
                    .cobDate(cobDate)
                    .sourceSystem(source)
                    .topic(notificationTopic)
                    .build();

            producer.sendNotification(notification);
            log.info("Notification sent: {} to {}", notification, notificationTopic);
        }
    }
}

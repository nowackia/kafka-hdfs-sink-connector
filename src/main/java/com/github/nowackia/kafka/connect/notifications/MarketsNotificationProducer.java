package com.github.nowackia.kafka.connect.notifications;

import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static kafka.admin.AdminUtils.createTopic;
import static kafka.admin.AdminUtils.topicExists;
import static kafka.utils.ZkUtils.apply;

public class MarketsNotificationProducer implements NotificationProducer {

    private static final Logger log = LoggerFactory.getLogger(MarketsNotificationProducer.class);

    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";
    private final Producer<String, OrchestrationNotification> producer;
    private final ZkUtils zkUtils;


    public MarketsNotificationProducer(Map<String, String> props, boolean isKerberosEnabled) {

        Properties zkProps = new Properties();
        zkProps.put(BOOTSTRAP_SERVERS, props.get(BOOTSTRAP_SERVERS));
        zkProps.put("acks", "all");
        zkProps.put("retries", props.getOrDefault("notification.retries", "3"));
        zkProps.put("value.serializer", "com.lbg.lake.kafka.connect.file.sink.notifications.MarketNotificationEncoder");
        zkProps.put("key.serializer", "org.apache.kafka.common.serialization.BytesSerializer");

        if(isKerberosEnabled) {
            log.info("Authenticating zookeeper for notifications using kerberos");
            zkProps.put("security.protocol", "PLAINTEXTSASL");
            zkProps.put("sasl.kerberos.service.name", "zookeeper");
        }

        producer = new KafkaProducer(zkProps);
        ZkClient zkClient = new ZkClient(props.get(ZOOKEEPER_SERVERS), 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = apply(zkClient, false);

    }

    /**
     * Sends MarketNotification Message
     * @param notification MarketNotification
     */
    @Override
    public void sendNotification(OrchestrationNotification notification) {
        if (!topicExists(zkUtils,notification.topic())) {
            createTopic(zkUtils, notification.topic(),
                    1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        }
        ProducerRecord<String, OrchestrationNotification> data = new ProducerRecord(notification.topic(), notification);
        producer.send(data);

    }

    @Override
    public void close() {
        producer.close();
    }
}

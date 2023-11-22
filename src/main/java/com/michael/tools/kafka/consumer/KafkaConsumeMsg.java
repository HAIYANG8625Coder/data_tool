package com.michael.tools.kafka.consumer;

import com.michael.tools.kafka.JavaKafkaConfigurer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaConsumeMsg {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumeMsg.class);

    private KafkaConsumer<String, String> consumer;
    private String topic;


    public KafkaConsumeMsg() {
        // sasl file path
        JavaKafkaConfigurer.configureSasl();

        // load kafka.properties
        Properties kafkaProperties =  JavaKafkaConfigurer.getKafkaProperties();

        Properties props = new Properties();
        // kafka server
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        // SASL_SSL protocol
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        // SASL account
        String saslMechanism = kafkaProperties.getProperty("sasl.mechanism");
        String username = kafkaProperties.getProperty("sasl.username");
        String password = kafkaProperties.getProperty("sasl.password");
        if (!JavaKafkaConfigurer.isEmpty(username)
                && !JavaKafkaConfigurer.isEmpty(password)) {
            String prefix = "org.apache.kafka.common.security.scram.ScramLoginModule";
            if ("PLAIN".equalsIgnoreCase(saslMechanism)) {
                prefix = "org.apache.kafka.common.security.plain.PlainLoginModule";
            }
            String jaasConfig = String.format("%s required username=\"%s\" password=\"%s\";", prefix, username, password);
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        }
        // SASL
        props.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        // between too polls
        // timeout interval
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        // single fetch volume
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 32000);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 32000);
        // single max fetch records
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        // message serializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));
        // hostname check
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        // Kafka content and topic
        this.topic = kafkaProperties.getProperty("topic"); //  topic  name


        // consumer
        this.consumer = new KafkaConsumer<>(props);
    }

    public void consumeMsg(){

        // set the topic for the consumer group, can set multiple consumers if it's needed
        // if GROUP_ID_CONFIG is same，better use the same topic
        List<String> subscribedTopics =  new ArrayList<String>();
        // add it here if you need to subscribe multiple topic
        // the topic needs to be created first
        subscribedTopics.add(this.topic);
        consumer.subscribe(subscribedTopics);

        //consuming data
        while (true){
            try {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                // needs to deal with all the data before next round poll and the consuming time is less than SESSION_TIMEOUT_MS_CONFIG
                // better user another thread poll to consume data and return the result asynchronously
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println(String.format("Consume partition:%d offset:%d record: %s", record.partition(), record.offset(), record.value()));
                    log.info("Consume partition: {} offset: {} record: {}", record.partition(), record.offset(), record.value());
                }
            } catch (Exception e) {
                try {
                    Thread.sleep(1000);
                } catch (Throwable ignore) {

                }
                e.printStackTrace();
            }
        }
    }


}

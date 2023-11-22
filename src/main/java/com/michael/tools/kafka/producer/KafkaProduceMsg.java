package com.michael.tools.kafka.producer;

import com.michael.tools.kafka.JavaKafkaConfigurer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProduceMsg {
    private static final Logger log = LoggerFactory.getLogger(KafkaProduceMsg.class);
    private Long count;
    private Long interval;

    private String topic;
    private KafkaProducer<String, String> producer;

    public String getSendValue() {
        return sendValue;
    }

    public void setSendValue(String sendValue) {
        this.sendValue = sendValue;
    }

    private String sendValue;

    public KafkaProduceMsg() {

        // init kafka
        // set the sasl file path
        JavaKafkaConfigurer.configureSasl();

        // load kafka.properties
        Properties kafkaProperties =  JavaKafkaConfigurer.getKafkaProperties();

        Properties props = new Properties();
        // kafka server
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");

        // set SASL
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

        // serializer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // max wait time of request
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
        // internal retry times
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        // internal retry interval
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);

        // set  verification of hostname empty
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        // Kafka content and topic
        this.topic = kafkaProperties.getProperty("topic"); //  topic  name

        // Producer object which is safe thread
        // if you need to improve performance, try to create multiple objects but not too much, keep it in 5
        this.producer = new KafkaProducer<String, String>(props);

    }

    public KafkaProduceMsg(Long count, Long interval) {
        this();
        this.count = count;
        this.interval = interval;
    }

    /***
     * send message with same value
     */
    public void sendMsg(){

        try {
            //use  futures to speed up
            List<Future<RecordMetadata>> futures = new ArrayList<Future<RecordMetadata>>(128);
            for (int j = 0; j < this.count/5; j++) {

                for (int i =0; i < 5; i++) {
                    // send message and get a Future object
                    ProducerRecord<String, String> kafkaMessage =  new ProducerRecord<String, String>(topic, this.sendValue + ": " + (i+j*5+1));
                    Future<RecordMetadata> metadataFuture = producer.send(kafkaMessage);
                    futures.add(metadataFuture);
                    // sleep interval second
                    if (this.interval > 0) {

                        try {
                            Thread.sleep(1000 * this.interval);
                        } catch (Throwable ignore) {

                        }
                    }
                }
                producer.flush();
                for (Future<RecordMetadata> future: futures) {
                    //get the sending message result
                    try {
                        RecordMetadata recordMetadata = future.get();
                        log.info("Produce ok: {}", recordMetadata);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }


            }

        } catch (Exception e) {
            // catch the exception after retrying
            log.error("error occurred: {}",e);
            e.printStackTrace();
        }
    }

    /***
     * send message with  data parser to get target string every round
     */
    public void sendMsg(DataParser dataParser){

        try {

            for (int i = 1; i <= this.count; i++) {
                ProducerRecord<String, String> kafkaMessage =  new ProducerRecord<String, String>(topic, dataParser.parseDataString() + ": " + i);
                Future<RecordMetadata> metadataFuture = producer.send(kafkaMessage);
                producer.flush();

                // sleep interval second
                if (this.interval > 0) {
                    try {
                        Thread.sleep(1000 * this.interval);
                    } catch (Throwable ignore) {
                    }
                }

                //get the sending message result
                try {
                    RecordMetadata recordMetadata = metadataFuture.get();
                    log.info("Produce ok: {}", recordMetadata);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }

        } catch (Exception e) {
            // catch the exception after retrying
            log.error("error occurred: {}",e);
            e.printStackTrace();
        }
    }


}

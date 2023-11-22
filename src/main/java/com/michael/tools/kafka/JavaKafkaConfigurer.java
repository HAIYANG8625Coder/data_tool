package com.michael.tools.kafka;

import java.util.Properties;

public class JavaKafkaConfigurer {

    private static Properties properties;

    public static boolean isEmpty(String str) {
       if (null == str) {
           return true;
       }
       if (0 == str.trim().length()) {
           return true;
       }
       return false;
    }

    public static void configureSasl() {
        //if it is set by the other ways like -D no need to set it
        Properties kafkaProperties = getKafkaProperties();
        if (null == System.getProperty("java.security.auth.login.config")
                && isEmpty(kafkaProperties.getProperty("sasl.username"))
                && isEmpty(kafkaProperties.getProperty("sasl.password"))) {
            //the path must be a path which is readable
            System.setProperty("java.security.auth.login.config", getKafkaProperties().getProperty("java.security.auth.login.config"));
        }
    }

    public synchronized static Properties getKafkaProperties() {
        if (null != properties) {
            return properties;
        }
        //get properties of kafka.properties
        Properties kafkaProperties = new Properties();
        try {
            kafkaProperties.load(JavaKafkaConfigurer.class.getClassLoader().getResourceAsStream("kafka/kafka.properties"));
        } catch (Exception e) {
            //exit if there is no config file
            e.printStackTrace();
        }
        properties = kafkaProperties;
        return kafkaProperties;
    }


}

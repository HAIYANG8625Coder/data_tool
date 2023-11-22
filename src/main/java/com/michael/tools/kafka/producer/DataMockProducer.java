package com.michael.tools.kafka.producer;

import cn.hutool.json.JSONObject;
import com.michael.tools.kafka.JsonUtilData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMockProducer {

    private static final Logger log = LoggerFactory.getLogger(DataMockProducer.class);

    public static void main(String[] args) {

        // 1. mock data plan
        // 1.1. get json object from meta file
        JSONObject dataInsertObject = JsonUtilData.readDataFromJSONFile("datagen/data_insert_setting.json");

        // 1.2. get data mock plan
        Long count = dataInsertObject.getLong("count");
        Long interval = dataInsertObject.getLong("interval");
        Boolean ifFreshData = dataInsertObject.getBool("fresh");

        // 2. get json object from meta file
        JSONObject jsonObject = JsonUtilData.readDataFromJSONFile("datagen/data_meta.json");

        // 2. generate the kafka data string
        DataParser dataParser = new DataParser(jsonObject);

        // 4. produce data to kafka
        KafkaProduceMsg sendKafkaMsg = new KafkaProduceMsg(count, interval);
        if (ifFreshData) {
            // send fresh data every round
            sendKafkaMsg.sendMsg(dataParser);
        } else {
            // send fix string
            String kafkaString = dataParser.parseDataString();
            log.info(kafkaString);
            sendKafkaMsg.setSendValue(kafkaString);
            sendKafkaMsg.sendMsg();
        }


    }
}

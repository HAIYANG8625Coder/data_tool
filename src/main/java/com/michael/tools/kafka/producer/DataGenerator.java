package com.michael.tools.kafka.producer;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


public class DataGenerator {

    private static final Logger log = LoggerFactory.getLogger(DataGenerator.class);

    /**
     * generate targeted data string based on the name, data type, data length, data range
     * @param name
     * @param dataType
     * @param dataLength
     * @param dataRange data range can be null, which means generating a fully random data
     * @return
     */
     public JSONObject dataGenerate(String name, String dataType, Integer dataLength, Object dataRange){

         JSONObject jsonObject = new JSONObject();
         Random random = new Random();

         switch (dataType) {
             case "string":
                 String generatedStr = StringGenerate(dataLength, dataRange);
                 jsonObject.putOnce(name, generatedStr);
                 break;
             case "integer":
                 log.info("generate a random string");
                 int i = random.nextInt(dataLength);
                 jsonObject.putOnce(name,i);
                 break;
             case "json":  // json
                 log.info("json string!!!");
                 if (StrUtil.isEmpty(name)) {
                     jsonObject.putAll((JSONObject)dataRange);
                 } else {
                     jsonObject.putOnce(name,dataRange);
                 }

                 break;
             default:
                log.error("the input data type: {} is not supported", dataType);

         }
         return jsonObject;
     }

    /***
     *  generate
     * @param dataType
     * @param dataLength
     * @param dataRange
     * @return
     */
    public Object dataGenerate(String dataType, Integer dataLength, Object dataRange){

        Object jsonObject = new Object();
        Random random = new Random();

        switch (dataType) {
            case "string":
                String generatedStr = StringGenerate(dataLength, dataRange);
                jsonObject= generatedStr;
                break;
            case "integer":
                log.info("generate a random string");
                int i = random.nextInt(dataLength);
                jsonObject= i;
                break;
            case "json":  // json
                log.info("json string!!!");
                jsonObject= dataRange;
                break;
            default:
                log.error("the input data type: {} is not supported", dataType);

        }
        return jsonObject;
    }


    /**
     * generate string based on data length and data range
     * @param dataLength data length
     * @param dataRange  data range which is usually used when generate a random string based on the char list
     * @return
     */
     private String StringGenerate(Integer dataLength, Object dataRange){

         StringBuilder result = new StringBuilder();
         Random random = new Random();
         String randomStr = "";
         String sourceStr = dataRange.toString();

         if ( dataLength <= 0) {
             log.error("the input data length: {} is not correct, please check!!!", dataLength);
         }
         if (sourceStr.length() == 0) {
             log.info("current random string is generated without a char list");
             randomStr = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ !@#$%^&*()";
         } else {
             log.info("current random string is generated based on the string : {}", dataRange);
             randomStr = sourceStr;
         }

         for (int i = 0; i < dataLength; i++) {
             int randomIndex = random.nextInt(randomStr.length());
             char randomChar = randomStr.charAt(randomIndex);
             result.append(randomChar);
         }

         return result.toString();
     }




}

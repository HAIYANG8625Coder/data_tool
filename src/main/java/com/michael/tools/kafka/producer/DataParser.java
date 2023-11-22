package com.michael.tools.kafka.producer;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * 1. parse the metadata
 * 2. generate the mock data
 */
public class DataParser {
    private static final Logger log = LoggerFactory.getLogger(DataParser.class);

    private JSONObject dataMetaObject;

    public DataParser(JSONObject dataMetaObject) {
        this.dataMetaObject = dataMetaObject;
    }

    /***
     * create the kafka data string
     * @return  parseDataString
     */
    public String parseDataString(){
        DataGenerator dataGenerator = new DataGenerator();
        JSONArray meta = dataMetaObject.getJSONArray("meta");
        String type = dataMetaObject.getStr("type").trim();

        if (StrUtil.equals(type,"json")) {
            JSONObject metaStr = new JSONObject();
            for (int i = 0; i < meta.size(); i++) {
                JSONObject jsonObject = meta.getJSONObject(i);
                JSONObject tpmResult = dataGenerator.dataGenerate(jsonObject.getStr("name"),
                        jsonObject.getStr("dataType"),
                        jsonObject.getInt("dataLength"),
                        jsonObject.getObj("dataRange"));
                metaStr.putAll(tpmResult);
            }
            return metaStr.toString();

        } else if (StrUtil.equals(type,"list")){
            JSONArray metaStr = new JSONArray();
            for (int i = 0; i < meta.size(); i++) {
                JSONObject jsonObject = meta.getJSONObject(i);
                Object tpmResult = dataGenerator.dataGenerate(
                        jsonObject.getStr("dataType"),
                        jsonObject.getInt("dataLength"),
                        jsonObject.getObj("dataRange"));
                metaStr.add(tpmResult);
            }
            return metaStr.toString();
        } else {
            log.error("type: {} is not supported right now!!!", type);
            return "";
        }

    }
}

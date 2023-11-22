package com.michael.tools.kafka;

import cn.hutool.core.io.IORuntimeException;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class JsonUtilData {

    private static final Logger log = LoggerFactory.getLogger(JsonUtilData.class);

    public static JSONObject readDataFromJSONFile(String path) {

        URL fileURL = JsonUtilData.class.getClassLoader().getResource(path);
//        System.out.println(fileURL+"");
        JSONObject jsonObject;
        try {
            File jsonFile = new File(fileURL.toURI());
            // read JSON content from the file
            jsonObject = JSONUtil.readJSONObject(jsonFile, StandardCharsets.UTF_8);
        } catch (IORuntimeException e) {
            log.error("read json file error: ", path);
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        return jsonObject;
    }
}

package com.insis.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * @author ZhuQichao
 * @create 2018/6/4 11:02
 **/
public class ConfigRead {
    private static Properties properties;
    static {
        properties = new Properties();
        InputStream in = ConfigRead.class.getResourceAsStream("config.properties");
        try {
            properties.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static String getProperty(String key){
        return properties.getProperty(key,null);
    }
    public static void setProperty(String key, String value) {
        properties.setProperty(key, value);
        // 保存文件
        try {
            URL fileUrl = ConfigRead.class.getClassLoader().getResource(
                    "config.properties");// 得到文件路径
            FileOutputStream fos = new FileOutputStream(new File(
                    fileUrl.toURI()));
            properties.store(fos, "the primary key of article table");
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        String v = getProperty("zkConnect");
        System.out.println(v);
    }
}

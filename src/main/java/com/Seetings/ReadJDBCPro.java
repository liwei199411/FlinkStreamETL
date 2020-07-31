package com.Seetings;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ReadJDBCPro {
    public static Properties buildGreenPlumJDBCProps(){
        Properties properties = new Properties();
        try {
            properties.load(new FileReader("database.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        properties.setProperty("url","jdbc:postgresql://10.252.97.201:5432/datahub?serverTimezone=GMT+8");
        properties.setProperty("Username","ur_uapp_data");
        properties.setProperty("Password","D@TAtstuapp201");
        return properties;
    }
    public static Properties buildMysqlJDBCProps(){
        Properties properties = new Properties();
        properties.setProperty("url","jdbc:mysql://master:3306/canal_destination??useUnicode=true&characterEncoding=UTF-8");
        properties.setProperty("Username","root");
        properties.setProperty("Password","root");
        return properties;
    }

}

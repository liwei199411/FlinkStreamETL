package com.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonFilter {
    public String IsDdl = "false";
    public String TableName = "t_meeting_info";
    public String Type = "INSERT";
    public String Data="data";
    public static String fieldDelimiter = ",";//字段分隔符，用于分隔Json解析后的字段

    public JsonFilter() {
    }

    public Boolean getJsonFilter(String string) {
        JSONObject record = JSON.parseObject(string, Feature.OrderedField);
        return record.getString("isDdl").equals(IsDdl) && record.getString("table").equals(TableName) && record.getString("type").equals(Type);
    }

    public String dataMap(String jsonvalue) throws Exception {
        StringBuilder fieldValue = new StringBuilder();
        JSONObject record = JSON.parseObject(jsonvalue, Feature.OrderedField);
        //获取最新的字段值
        JSONArray data = record.getJSONArray(Data);
        //遍历，字段值的JSON数组，只有一个元素
        for (int i = 0; i < data.size(); i++) {
            //获取data数组的所有字段
            JSONObject obj = data.getJSONObject(i);
            if (obj != null) {
                for (Map.Entry<String, Object> entry : obj.entrySet()) {
                    fieldValue.append(entry.getValue());
                    fieldValue.append(fieldDelimiter);
                }
            }
        }
        return fieldValue.toString();
    }

    public Tuple5<Integer,String,Integer, String, String> fieldMap(String datafield) throws Exception {
        Integer meeting_id= Integer.valueOf(datafield.split("[\\,]")[0]);
        String meeting_code=datafield.split("[\\,]")[1];
        Integer address_id= Integer.valueOf(datafield.split("[\\,]")[7]);
        String mstart_date=datafield.split("[\\,]")[13];
        String mend_date=datafield.split("[\\,]")[14];
        return new Tuple5<Integer, String,Integer,String, String>(meeting_id, meeting_code,address_id,mstart_date,mend_date) ;
    }
}

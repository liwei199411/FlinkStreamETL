package com.Seetings;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
/**
 会议室维表同步
 目前存在的问题：JDBCInputFormat一次性拿全量数据放入state中，无法感知mysql维表的变化，也会占用大量的state空间。
 后面需要实现用async io+cache+异步jdbc 才可以
 维表
 */
public class CreateJDBCInputFormat {
    TypeInformation<?>[] fieldTypes=new TypeInformation<?>[]{
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO
    };
    RowTypeInfo rowTypeInfo=new RowTypeInfo(fieldTypes);
    public JDBCInputFormat createJDBCInputFormat(){
        JDBCInputFormat jdbcInputFormat=JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://master/canal_test")
                .setUsername("root")
                .setPassword("root")
                .setQuery("SELECT tma.id AS meetingroom_id,tma.name as meetingroom_name,tma.location as location_id,tml.full_name as location_name,tmr.`name` AS city\n" +
                        "FROM t_meeting_address as tma LEFT JOIN t_meeting_location AS tml \n" +
                        "ON tma.location=tml.code \n" +
                        "LEFT JOIN t_meeting_region AS tmr ON tml.region_id=tmr.id") //维表
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        return jdbcInputFormat;
    }
}

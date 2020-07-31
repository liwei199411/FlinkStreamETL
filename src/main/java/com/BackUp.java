package com;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.model.Meeting;
import com.sinks.SinkToGreenplum;
import com.Seetings.CreateJDBCInputFormat;
import com.utils.KafkaConfigUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * 备份
 * */
public class BackUp {
    private static Logger log = LoggerFactory.getLogger(BackUp.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
        env.enableCheckpointing(1000);////非常关键，一定要设置启动检查点
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设置事件时间
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        String fieldDelimiter =KafkaConfigUtil.fieldDelimiter;

        EnvironmentSettings bsSettings=EnvironmentSettings.newInstance()//使用Blink planner、创建TableEnvironment,并且设置状态过期时间，避免Job OOM
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);
        tEnv.getConfig().setIdleStateRetentionTime(Time.days(1),Time.days(2));
/**
 设置空闲state的保留时间，这个也需要考虑一下
 StreamQueryConfig queryConfig = tEnv.queryConfig();
 queryConfig.withIdleStateRetentionTime(Time.days(10), Time.days(30));
 */
        Properties properties = KafkaConfigUtil.buildKafkaProps();//kafka参数配置

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaConfigUtil.topic, new SimpleStringSchema(), properties);
/**
 consumer.setStartFromEarliest(); 测试没问题之后，可以考虑删除
 consumer.setStartFromGroupOffsets();
 env.addSource(consumer).print();//在控制台查看Kafka中的Json格式的字符串
 */
        //将Kafka-consumer的数据作为源，并对Json格式进行转解析，用到了阿里的fastjson
        SingleOutputStreamOperator<Tuple5<Integer,String,Integer,String,String>> meeting_stream=env.addSource(consumer)
                .filter(new FilterFunction<String>() { //过滤掉JSON格式中的DDL操作
                    @Override
                    public boolean filter(String jsonVal) throws Exception {
                        JSONObject record= JSON.parseObject(jsonVal, Feature.OrderedField);
                        //json格式："isDdl":false
                        return record.getString("isDdl").equals("false");
                    }
                })
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String jsonvalue) throws Exception {
                        StringBuilder fieldsBuilder=new StringBuilder();
                        StringBuilder fieldValue=new StringBuilder();
                        //解析Json数据
                        JSONObject record=JSON.parseObject(jsonvalue,Feature.OrderedField);
                        //获取最新的字段值
                        JSONArray data=record.getJSONArray("data");
                        //遍历，字段值得JSON数组，只有一个元素
                        for (int i = 0; i <data.size() ; i++) {
                            //获取data数组的所有字段
                            JSONObject obj=data.getJSONObject(i);
                            if(obj!=null){
                                fieldsBuilder.append(record.getLong("id"));//序号id
                                fieldsBuilder.append(fieldDelimiter);//字段分隔符
                                fieldsBuilder.append(record.getLong("es"));//业务时间戳
                                fieldsBuilder.append(fieldDelimiter);
                                fieldsBuilder.append(record.getLong("ts"));//日志时间戳
                                fieldsBuilder.append(fieldDelimiter);
                                fieldsBuilder.append(record.getString("type"));//操作类型，包含Insert，Delete，Update
                                for(Map.Entry<String,Object> entry:obj.entrySet()){
                                    fieldValue.append(entry.getValue());
                                    fieldValue.append(fieldDelimiter);
//                                    fieldsBuilder.append(fieldDelimiter);
//                                    fieldsBuilder.append(entry.getValue());//获取表字段数据
                                }
                            }
                        }
                        return fieldValue.toString();
                    }
                }).map(new MapFunction<String, Tuple5<Integer,String,Integer, String, String>>() {
                    @Override
                    public Tuple5<Integer,String,Integer, String, String> map(String field) throws Exception {
                        Integer meeting_id= Integer.valueOf(field.split("[\\,]")[0]);
                        String meeting_code=field.split("[\\,]")[1];
                        Integer address_id= Integer.valueOf(field.split("[\\,]")[7]);
                        String mstart_date=field.split("[\\,]")[13];
                        String mend_date=field.split("[\\,]")[14];
                        return new Tuple5<Integer, String,Integer,String, String>(meeting_id, meeting_code,address_id,mstart_date,mend_date) ;
                    }
                });
        //将流式数据（元组类型）注册为表
        tEnv.registerDataStream("meeting_info",meeting_stream,"meeting_id, meeting_code,address_id,mstart_date,mend_date,proctime.proctime");
        //会议室维表同步
        CreateJDBCInputFormat createJDBCFormat=new CreateJDBCInputFormat();
        JDBCInputFormat jdbcInputFormat=createJDBCFormat.createJDBCInputFormat();
        DataStreamSource<Row> dataStreamSource=env.createInput(jdbcInputFormat);//字段类型
        tEnv.registerDataStream("meeting_address",dataStreamSource,"meetingroom_id,meetingroom_name,location_id,location_name,city");

        //流表与维表join,并对结果表进行查询
        Table meeting_info=tEnv.scan("meeting_info");
        Table meeting_address=tEnv.sqlQuery("SELECT * FROM meeting_address");

        Table joined=tEnv.sqlQuery("SELECT mi.meeting_id, mi.meeting_code,ma.meetingroom_id,ma.meetingroom_name,ma.location_name,ma.city" +
                " FROM meeting_info AS mi " +
                "LEFT JOIN " +
                "meeting_address AS ma " +
                "ON mi.address_id=ma.meetingroom_id");
/**
 对结果表进行查询,TO_TIMESTAMP是Flink的时间函数，对时间格式进行转换，具体请看官网
 只对开始的会议进行转换。   统计空置率指的是统计当下时间里，已经在会议中的会议室，还是已经预定的呢
 Table joined=tEnv.sqlQuery("select meeting_id, meeting_code,TO_TIMESTAMP(mstart_date),TO_TIMESTAMP(mend_date),proctime.proctime " +
 "from meeting_info " +
 "where TO_TIMESTAMP(mstart_date)<LOCALTIMESTAMP<TO_TIMESTAMP(mend_date)");

 SQL解析过程
 String explanation = tEnv.explain(joined);
 System.out.println(explanation);

 适用于维表查询的情况1
 DataStream<Tuple2<Boolean,Row>> stream1 =tEnv.toRetractStream(joined,Row.class).filter(new FilterFunction<Tuple2<Boolean, Row>>() {
@Override
public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
return booleanRowTuple2.f0;
}
});
 stream1.print();
 */

//        适用于维表查询的情况2
        DataStream<Tuple2<Boolean,Row>> stream_tosink =tEnv.toRetractStream(joined,Row.class);
        stream_tosink.process(new ProcessFunction<Tuple2<Boolean, Row>, Object>() {
            private Tuple2<Boolean, Row> booleanRowTuple2;
            private ProcessFunction<Tuple2<Boolean, Row>, Object>.Context context;
            private Collector<Object> collector;
            @Override
            public void processElement(Tuple2<Boolean, Row> booleanRowTuple2, Context context, Collector<Object> collector) throws Exception {
                if(booleanRowTuple2.f0){
                    System.out.println(JSON.toJSONString(booleanRowTuple2.f1));
                }
            }
        });
        stream_tosink.print();//测试输出

        //转换Tuple元组到实体类对象
        DataStream<Meeting> dataStream=stream_tosink.map(new MapFunction<Tuple2<Boolean, Row>, Meeting>() {
            @Override
            public Meeting map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                Meeting meeting=new Meeting();
                meeting.setMeeting_id((Integer) booleanRowTuple2.f1.getField(0));
                System.out.println("meeting_id:"+booleanRowTuple2.f1.getField(0));

                meeting.setMeeting_code((String) booleanRowTuple2.f1.getField(1));
                System.out.println("meeting_code:"+booleanRowTuple2.f1.getField(1));

                meeting.setMeetingroom_id((Integer) booleanRowTuple2.f1.getField(2));
                System.out.println("meetingroom_id:"+booleanRowTuple2.f1.getField(2));

                meeting.setMeetingroom_name((String) booleanRowTuple2.f1.getField(3));
                System.out.println("meetingroom_name:"+booleanRowTuple2.f1.getField(3));

                meeting.setLocation_name((String) booleanRowTuple2.f1.getField(4));
                System.out.println("location_name:"+booleanRowTuple2.f1.getField(4));

                meeting.setCity((String) booleanRowTuple2.f1.getField(5));
                System.out.println("city:"+booleanRowTuple2.f1.getField(5));

                return meeting;
            }
        });
        dataStream.print();
//      dataStream.addSink(new SinkMeetingToMySQL()); //测试ok
//      dataStream.addSink(new SinkToMySQL());//测试ok
        dataStream.addSink(new SinkToGreenplum());

        //执行
        env.execute("Meeting Streaming job");
    }
}
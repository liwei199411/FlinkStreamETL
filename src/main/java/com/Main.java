package com;

import com.Seetings.DimensionTableSeetings;
import com.alibaba.fastjson.JSON;
import com.model.Meeting;
import com.sinks.SinkToGreenplum;
import com.Seetings.CreateJDBCInputFormat;
import com.sqlquery.DimensionSQLQuery;
import com.sqlquery.JoinedSQLQuery;
import com.utils.JsonFilter;
import com.utils.KafkaConfigUtil;
import com.Seetings.StreamTableSeetings;
import com.utils.Tuple2ToMeeting;
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
import java.util.Properties;

/**
 * Flink 实时计算MysqlBinLog日志，并写入数据库
 * */
public class Main {
    private static Logger log = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        /**
         *   Flink 配置
         * */
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();  //设置此可以屏蔽掉日记打印情况
        env.enableCheckpointing(1000);////非常关键，一定要设置启动检查点
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设置事件时间
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        EnvironmentSettings bsSettings=EnvironmentSettings.newInstance()//使用Blink planner、创建TableEnvironment,并且设置状态过期时间，避免Job OOM
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);
        tEnv.getConfig().setIdleStateRetentionTime(Time.days(1),Time.days(2));
        /**
         *   Kafka配置
         * */
        Properties properties = KafkaConfigUtil.buildKafkaProps();//kafka参数配置
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(KafkaConfigUtil.topic, new SimpleStringSchema(), properties);
        /**
         *   将Kafka-consumer的数据作为源
         *   并对Json格式进行解析
         * */
        SingleOutputStreamOperator<Tuple5<Integer,String,Integer,String,String>> meeting_stream=env.addSource(consumer)
                .filter(new FilterFunction<String>() { //过滤掉JSON格式中的DDL操作
                    @Override
                    public boolean filter(String jsonVal) throws Exception {
                        //json格式解析："isDdl":false,"table":t_meeting_info,"type":"INSERT"
                        return new JsonFilter().getJsonFilter(jsonVal);
                    }
                })
                .map(new MapFunction<String, String>() {
                    @Override
                    //获取字段数据
                    public String map(String jsonvalue) throws Exception {
                        return new JsonFilter().dataMap(jsonvalue);
                    }
                }).map(new MapFunction<String, Tuple5<Integer,String,Integer, String, String>>() {
                    @Override
                    public Tuple5<Integer,String,Integer, String, String> map(String dataField) throws Exception {
                        return new JsonFilter().fieldMap(dataField);
                    }
                });
        /**
         *   将流式数据（元组类型）注册为表
         *   会议室维表同步
         */
        tEnv.registerDataStream(StreamTableSeetings.streamTableName,meeting_stream,StreamTableSeetings.streamField);
        CreateJDBCInputFormat createJDBCFormat=new CreateJDBCInputFormat();
        JDBCInputFormat jdbcInputFormat=createJDBCFormat.createJDBCInputFormat();
        DataStreamSource<Row> dataStreamSource=env.createInput(jdbcInputFormat);//字段类型
        tEnv.registerDataStream(DimensionTableSeetings.DimensionTableName,dataStreamSource,DimensionTableSeetings.DimensionTableField);

        //流表与维表join,并对结果表进行查询
        Table meeting_info=tEnv.scan(StreamTableSeetings.streamTableName);
        Table meeting_address=tEnv.sqlQuery(DimensionSQLQuery.Query);
        Table joined=tEnv.sqlQuery(JoinedSQLQuery.Query);
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
        //适用于维表查询的情况2
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
                return new Tuple2ToMeeting().getTuple2ToMeeting(booleanRowTuple2);
            }
        });
        /**
         *   Sink
         * */
        dataStream.print();
        //dataStream.addSink(new SinkToMySQL());//测试ok
        dataStream.addSink(new SinkToGreenplum());//测试ok
        //执行
        env.execute("Meeting Streaming job");
    }
}
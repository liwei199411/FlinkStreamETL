package com.sinks;
import com.Seetings.ReadJDBCPro;
import com.model.Meeting;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * sink to Greenplum
 * */

public class SinkToGreenplum extends RichSinkFunction<Meeting>{
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;
    /**
     * open() 方法中建立连接，这样不用每次invoke的时候都要建立连接和释放连接
     * @param parameters
     * @throws Exception
     * */

    @Override
    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        dataSource=new BasicDataSource();
        connection=getConnection(dataSource);
        String sql="INSERT INTO public .meeting_result(meeting_id, meeting_code, meetingroom_id,meetingroom_name,location_name,city) values(?, ?, ?,?,?,?);";
        ps=this.connection.prepareStatement(sql);
    }
    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if(connection!=null){
            connection.close();
        }
        if(ps!=null){
            connection.close();
        }
    }

    /**
     * 每条数据的插入都需要调用一次invoke()方法
     * @param meeting
     * @param context
     * @throws Exception
     * */
    @Override
    public void invoke(Meeting meeting,Context context) throws Exception{
        ps.setInt(1,meeting.getMeeting_id());
        ps.setString(2,meeting.getMeeting_code());
        ps.setInt(3,meeting.getMeetingroom_id());
        ps.setString(4,meeting.getMeetingroom_name());
        ps.setString(5,meeting.getLocation_name());
        ps.setString(6,meeting.getCity());
        ps.executeUpdate();
        System.out.println("插入成功:"+meeting.toString());
    }

    private static  Connection getConnection(BasicDataSource dataSource) {
        Properties prop=new Properties();
        try {
            prop.load(new FileInputStream("D:\\flink\\src\\main\\resources\\database.properties"));
            String driver=prop.getProperty("driver");
            String url=prop.getProperty("url");
            String username=prop.getProperty("Username");
            String password=prop.getProperty("Password");

            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //设置连接池的参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con=null;
        try{
            con=dataSource.getConnection();
            System.out.println("创建连接池："+con);
        } catch (Exception e) {
            System.out.println("-----------greenplum get connection has exception,msg=" +e.getMessage());
        }
        return con;
    }
}
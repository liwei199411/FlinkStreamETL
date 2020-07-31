package com.utils;

import com.model.Meeting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
/**
 * 将Tuple转为实体类对象
 * */
public class Tuple2ToMeeting {
    public Meeting getTuple2ToMeeting(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
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
}

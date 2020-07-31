package com.sqlquery;
/**
 * 流式表与维度表Join
 * */
public class JoinedSQLQuery {
    public static String Query="SELECT mi.meeting_id, mi.meeting_code,ma.meetingroom_id,ma.meetingroom_name,ma.location_name,ma.city" +
            " FROM meeting_info AS mi " +
            "LEFT JOIN " +
            "meeting_address AS ma " +
            "ON mi.address_id=ma.meetingroom_id";
}

package com.model;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Meeting 实体类对象
 * */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Meeting {
    public int meeting_id;
    public String meeting_code;
    public int meetingroom_id;
    public String meetingroom_name;
    public String location_name;
    public String city;
}

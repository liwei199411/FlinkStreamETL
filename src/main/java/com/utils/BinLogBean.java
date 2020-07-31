package com.utils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 暂时没有用到
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class BinLogBean implements Serializable{
    public String data;//最新的数据，为JSON数组，如果是插入则表示最新插入的数据；如果是更新，则表示更新后的最新数据；如果是删除，则表示被删除的数据
    public String database;//数据库
    public Long es;//事件时间，13位的时间戳
    public Long id;//事件操作的序列号，1，2,3
    public Boolean isDdl;//是否是DDL操作
    public String mysqlType; //字段类型
    public String old;//旧数据
    public String sql;//SQL 语句
    public String sqlType;// 经过Canal转换处理的，unsigned int 会被转化为Long，unsigned long会被转换为BigDecimal
    public String table;//TableName
    public Long ts;//日志时间戳
    public String type;//操作类型，包含Insert，Delete，Update
}
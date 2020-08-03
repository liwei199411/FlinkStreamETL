# FlinkStreamETL

## 0.功能说明
**概括**：利用Flink实时统计Mysql数据库BinLog日志数据，并将流式数据注册为流表，利用Flink SQL将流表与Mysql的维表进行JOIN，最后将计算结果实时写入Greenplum/Mysql。

## 1.需求分析

### **1.1需求**

实时统计各个地区会议室的空置率，预定率，并在前端看板上实时展示。源系统的数据库是`Mysql`，它有三张表，分别是：t_meeting_info（会议室预定信息表）、t_meeting_location（属地表，维度表）、t_meeting_address(会议室属地表，维度表）。

### **1.2说明**

`t_meeting_info`表中的数据每时每刻都在更新数据，若通过**`JDBC`**方式定时查询`Mysql`，会给源系统数据库造成大量无形的压力，甚至会影响正常业务的使用，并且时效性也不高。需要在基本不影响**`Mysql`**正常使用的情况下完成对增量数据的处理。

上面三张表的`DDL`语句如下：

- t_meeting_info（会议室预定信息表，这张表数据会实时更新）

```sql
CREATE TABLE `t_meeting_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `meeting_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '会议业务唯一编号',
  `msite` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议名称',
  `mcontent` varchar(4096) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议内容',
  `attend_count` int(5) DEFAULT NULL COMMENT '参会人数',
  `type` int(5) DEFAULT NULL COMMENT '会议类型 1 普通会议 2 融合会议 3 视频会议 4 电话会议',
  `status` int(255) DEFAULT NULL COMMENT '会议状态 ',
  `address_id` int(11) DEFAULT NULL COMMENT '会议室id',
  `email` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人邮箱',
  `contact_tel` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '联系电话',
  `create_user_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人姓名',
  `create_user_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人工号',
  `creator_org` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人组织',
  `mstart_date` datetime DEFAULT NULL COMMENT '会议开始时间',
  `mend_date` datetime DEFAULT NULL COMMENT '会议结束时间',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新人',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `company` int(10) DEFAULT NULL COMMENT '会议所在属地code',
  `sign_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '预留字段',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `t_meeting_info_meeting_code_index` (`meeting_code`) USING BTREE,
  KEY `t_meeting_info_address_id_index` (`address_id`) USING BTREE,
  KEY `t_meeting_info_create_user_id_index` (`create_user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=65216 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='会议主表';
```

- t_meeting_location（属地表，地区维表）

  ```sql
  CREATE TABLE `t_meeting_location` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `short_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属地简称',
    `full_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属地全称',
    `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属地code',
    `region_id` int(11) DEFAULT NULL COMMENT '地区id',
    `create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
    `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新人',
    `create_time` datetime DEFAULT NULL COMMENT '创建时间',
    `update_time` datetime DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`) USING BTREE,
    UNIQUE KEY `t_meeting_location_code_uindex` (`code`) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=103 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='属地表';
  ```

- t_meeting_address(会议室属地表，会议室维表)

  ```sql
  CREATE TABLE `t_meeting_address` (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
    `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议室名称',
    `location` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '所在属地',
    `shared` int(3) DEFAULT NULL COMMENT '是否共享 0 默认不共享 1 全部共享 2 选择性共享',
    `cost` int(10) DEFAULT NULL COMMENT '每小时成本',
    `size` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议室容量大小',
    `bvm_ip` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'BVM IP',
    `type` int(2) DEFAULT NULL COMMENT '会议室类型 1 普通会议室  2 视频会议室',
    `create_time` datetime DEFAULT NULL COMMENT '创建时间',
    `create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
    `update_time` datetime DEFAULT NULL COMMENT '更新时间',
    `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新人',
    `status` int(2) DEFAULT NULL COMMENT '是否启用 ，0 未启用 1已启用 2已删除',
    `order` int(5) DEFAULT NULL COMMENT '排序',
    `approve` int(2) DEFAULT NULL COMMENT '是否审批 0 不审批 1 审批',
    PRIMARY KEY (`id`) USING BTREE,
    KEY `t_meeting_address_location_index` (`location`) USING BTREE,
    KEY `order` (`order`) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=554 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='会议室表';
  ```

## 2.实现方案

方案如下图所示：

- 利用**Canal**监听**`Mysql`**数据库的增量`BinLog`日志数据（`JSON格式`）
- 将增量日志数据作为**Kafka**的生产者，Flink解析**Kafka**的`Topic` 中的数据并消费
- 将计算后的流式数据（Stream）注册为Flink 中的表（Table）
- 最后利用Flink与t_meeting_location、t_meeting_address维表进行JOIN，将最终的结果写入数据库。

![img](%E4%BC%9A%E8%AE%AE%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E5%AE%9E%E6%97%B6%E5%88%86%E6%9E%90%E6%80%BB%E7%BB%93.assets/%E6%80%BB%E4%BD%93%E6%96%B9%E6%A1%88.png)

需要**服务器：CentOS7，JDK8、Scala 2.12.6、Mysql、Canal、Flink1.9、Zookkeeper、Kafka**

### 2.1 Canal简介

**Canal**是阿里巴巴开源的纯`java`开发的基于数据库`binlog`的增量订阅&消费组件。Canal的原理是模拟为一个`Mysql slave`的交互协议，伪装为`MySQL slave`，向`Mysql Master`发送dump协议，然后`Mysql master`接收到这个请求后将`binary log`推送给slave(也就是Canal)，Canal解析binary log对象。

#### 2.1 Canal安装并配置(Cent Os服务器上)

**`Mysql`数据库配置**

- a. 开启`Mysql`的`Binlog`，修改`/etc/my.cnf`，在`[mysqld]`下添加如下配置，改完之后重启 `Mysql`,命令是： /etc/init.d/mysql restart。

```shell
[mysqld]
#添加这一行就ok
log-bin=mysql-bin
#选择row模式
binlog-format=ROW
#配置mysql replaction需要定义，不能和canal的slaveId重复
server_id=1
```

- b.创建一个`Mysql`用户并赋予相应的权限，用于**Canal**使用

  ```sql
  mysql>  CREATE USER canal IDENTIFIED BY 'canal';  
  mysql>  GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
  mysql>  GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
  mysql>  FLUSH PRIVILEGES;
  ```

- c.`Zookeeper`

  安装Kafka时需要依赖于`Zookeeper`（CDH 6.2版本已安装）

- d.安装Kafka，创建一个Topic

  ```shell
  kafka-topics.sh --create --zookeeper master:2181,slave01:2181,slave02:2181 --partitions 2 --replication-factor 1 --topic example
  ```

**Canal安装**：

- Canal下载地址

```shell
https://github.com/alibaba/canal/releases/tag/canal-1.1.2
```

- 解压（在解压之前创建一个canal目录，直接解压会覆盖文件）

```shell
mkdir -p /usr/local/canal
mv canal.deployer-1.1.2.tar.gz /usr/local/canal/
tar -zxvf canal.deployer-1.1.2.tar.gz
```

- 修改instance配置文件（在`/usr/local/canal/conf/example/instance.properties`下）

```shell
## mysql serverId , v1.0.26+ will autoGen ， 不要和server_id重复
canal.instance.mysql.slaveId=3
    
# position info。设置要监听的Mysql数据库的url
canal.instance.master.address=10.252.70.6:3306
    
# table meta tsdb info
canal.instance.tsdb.enable=false
    
# 这里配置前面在Mysql分配的用户名和密码
canal.instance.dbUsername=canal
canal.instance.dbPassword=canal
canal.instance.connectionCharset=UTF-8
# 配置需要检测的库名，可以不配置，这里只检测canal_test库
canal.instance.defaultDatabaseName=canal_test
# enable druid Decrypt database password
canal.instance.enableDruid=false
    
# 配置过滤的正则表达式，监测canal_test库下的所有表
canal.instance.filter.regex=canal_test\\..*
    
# 配置MQ
## 配置上在Kafka创建的那个Topic名字
canal.mq.topic=example
## 配置分区编号为1()
canal.mq.partition=1
```

- 修改canal.properties配置文件 

```shell
 
vim $CANAL_HOME/conf/canal.properties，修改如下项，其他默认即可
# 这个是如果开启的是tcp模式，会占用这个11111端口，canal客户端通过这个端口获取数据
canal.port = 11111
    
# 可以配置为：tcp, kafka, RocketMQ，这里配置为kafka
canal.serverMode = kafka
 
 ##################################################
#########              destinations      #############
##################################################

# 这里将这个注释掉，否则启动会有一个警告
#canal.instance.tsdb.spring.xml = classpath:spring/tsdb/h2-tsdb.xml
    
##################################################
#########              MQ              #############
##################################################
##Kafka集群
canal.mq.servers = master:9092,slave01:9092,slave02:9092
canal.mq.retries = 0
canal.mq.batchSize = 16384
canal.mq.maxRequestSize = 1048576
canal.mq.lingerMs = 1
canal.mq.bufferMemory = 33554432
# Canal的batch size, 默认50K, 由于kafka最大消息体限制请勿超过1M(900K以下)
canal.mq.canalBatchSize = 50
# Canal get数据的超时时间, 单位: 毫秒, 空为不限超时
canal.mq.canalGetTimeout = 100
# 是否为flat json格式对象
canal.mq.flatMessage = true
canal.mq.compressionType = none
canal.mq.acks = all
# kafka消息投递是否使用事务
#canal.mq.transaction = false
```

**启动Canal**

```shell
$CANAL_HOME/bin/startup.sh
```

logs下会生成两个日志文件：logs/canal/canal.log、logs/example/example.log，查看这两个日志，保证没有报错日志。

```shell
tail -f $CANAL_HOME/logs/example/example.log 
tail -f $CANAL_HOME/logs/canal/canal.log 
```

**测试一下**

在`Mysql`数据库中进行增删改查的操作，然后查看Kafka的topic为 example 的数据

```shell
  kafka-console-consumer.sh --bootstrap-server master:9092,slave01:9092,slave02:9092 --from-beginning --topic example
```

- 向Mysql数据库中插入几条数据

![image-20200710154201590](%E4%BC%9A%E8%AE%AE%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E5%AE%9E%E6%97%B6%E5%88%86%E6%9E%90%E6%80%BB%E7%BB%93.assets/image-20200710154201590.png)

- 在Kafka中查看这些插入的数据

![image-20200710154343692](%E4%BC%9A%E8%AE%AE%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E5%AE%9E%E6%97%B6%E5%88%86%E6%9E%90%E6%80%BB%E7%BB%93.assets/image-20200710154343692.png)

- 删除几条数据

  ![image-20200710154514531](%E4%BC%9A%E8%AE%AE%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E5%AE%9E%E6%97%B6%E5%88%86%E6%9E%90%E6%80%BB%E7%BB%93.assets/image-20200710154514531.png)

  **关闭Canal**

```shell
  # 不用的时候一定要通过这个命令关闭，如果是用kill或者关机，当再次启动依然会提示要先执行stop.sh脚本后才能再启动。
  $CANAL_HOME/bin/stop.sh
```

  **备注：**如果我们不使用Kafka作为Canal客户端，我们也可以用代码编写自己的Canal客户端，然后在代码中指定我们的数据去向。此时只需要将canal.properties配置文件中的`canal.serverMode`值改为`tcp`。**编写我们的客户端代码。**

### 2.2.实时计算框架去消费Kafka中的数据（Flink）

通过上一步已经可以获取到`cannal_test`库中的增量数据，并且可以将变化的数据实时推送到Kafka中。Kafka接收到的数据是一条`Json`格式的数据。我们需要对 INSERT 和 UPDATE 类型的数据处理。

#### 2.2.1 源系统表信息

`Mysql`数据建表语句如下：

- t_meeting_info（会议室预定信息表，这张表数据会实时更新）

```sql
CREATE TABLE `t_meeting_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `meeting_code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '会议业务唯一编号',
  `msite` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议名称',
  `mcontent` varchar(4096) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议内容',
  `attend_count` int(5) DEFAULT NULL COMMENT '参会人数',
  `type` int(5) DEFAULT NULL COMMENT '会议类型 1 普通会议 2 融合会议 3 视频会议 4 电话会议',
  `status` int(255) DEFAULT NULL COMMENT '会议状态 ',
  `address_id` int(11) DEFAULT NULL COMMENT '会议室id',
  `email` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人邮箱',
  `contact_tel` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '联系电话',
  `create_user_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人姓名',
  `create_user_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人工号',
  `creator_org` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人组织',
  `mstart_date` datetime DEFAULT NULL COMMENT '会议开始时间',
  `mend_date` datetime DEFAULT NULL COMMENT '会议结束时间',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新人',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `company` int(10) DEFAULT NULL COMMENT '会议所在属地code',
  `sign_status` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '预留字段',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `t_meeting_info_meeting_code_index` (`meeting_code`) USING BTREE,
  KEY `t_meeting_info_address_id_index` (`address_id`) USING BTREE,
  KEY `t_meeting_info_create_user_id_index` (`create_user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=65216 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='会议主表';
```

- t_meeting_location（属地表，地区维表）

  ```sql
  CREATE TABLE `t_meeting_location` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `short_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属地简称',
    `full_name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '属地全称',
    `code` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '属地code',
    `region_id` int(11) DEFAULT NULL COMMENT '地区id',
    `create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
    `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新人',
    `create_time` datetime DEFAULT NULL COMMENT '创建时间',
    `update_time` datetime DEFAULT NULL COMMENT '更新时间',
    PRIMARY KEY (`id`) USING BTREE,
    UNIQUE KEY `t_meeting_location_code_uindex` (`code`) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=103 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='属地表';
  ```

- t_meeting_address(会议室属地表，会议室维表)

  ```sql
  CREATE TABLE `t_meeting_address` (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
    `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议室名称',
    `location` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '所在属地',
    `shared` int(3) DEFAULT NULL COMMENT '是否共享 0 默认不共享 1 全部共享 2 选择性共享',
    `cost` int(10) DEFAULT NULL COMMENT '每小时成本',
    `size` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '会议室容量大小',
    `bvm_ip` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT 'BVM IP',
    `type` int(2) DEFAULT NULL COMMENT '会议室类型 1 普通会议室  2 视频会议室',
    `create_time` datetime DEFAULT NULL COMMENT '创建时间',
    `create_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '创建人',
    `update_time` datetime DEFAULT NULL COMMENT '更新时间',
    `update_user` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL COMMENT '更新人',
    `status` int(2) DEFAULT NULL COMMENT '是否启用 ，0 未启用 1已启用 2已删除',
    `order` int(5) DEFAULT NULL COMMENT '排序',
    `approve` int(2) DEFAULT NULL COMMENT '是否审批 0 不审批 1 审批',
    PRIMARY KEY (`id`) USING BTREE,
    KEY `t_meeting_address_location_index` (`location`) USING BTREE,
    KEY `order` (`order`) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=554 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='会议室表';
  ```

#### 2.2.2 解析`JSON`格式日志

**`Json`格式示例**

- Insert

  ```json
  {
      "data":[{
         		 "id":"18",
               "meeting_code":"M201907080071",
               "msite":"项目会议",		         
               "mcontent":"1",
               "attend_count":"5",
               "type":"1",
               "status":"5",
               "address_id":"72",
               "email":"*******",
               "contact_tel":"+86 *******",
               "create_user_name":"*******",
               "create_user_id":"*******",
               "creator_org":"*******",
               "mstart_date":"2019-07-19 08:30:00",
               "mend_date":"2019-07-19 18:00:00",
               "create_time":"2019-07-08 08:37:07",         
               "update_user":null,
               "update_time":null,
               "company":"100",
               "sign_status":null
              }],
      "database":"canal_test",
     	"es":1595491574000,
      "id":41327,
      "isDdl":false,
      "mysqlType":{
          		"id":"int(11)",
                   "meeting_code":"varchar(255)",
                   "msite":"varchar(255)",
                   "mcontent":"varchar(4096)",
                   "attend_count":"int(5)",
                   "type":"int(5)",
                   "status":"int(255)",
                   "address_id":"int(11)",
                   "email":"varchar(255)",
                   "contact_tel":"varchar(255)",
                   "create_user_name":"varchar(255)",
                  "create_user_id":"varchar(100)",
                   "creator_org":"varchar(255)",
                   "mstart_date":"datetime",
                   "mend_date":"datetime",
                   "create_time":"datetime",
                   "update_user":"varchar(255)",
                   "update_time":"datetime",
                   "company":"int(10)",
                   "sign_status":"varchar(255)"
                  },
      "old":null,
      "sql":"",
      "sqlType":{	"id":4,"meeting_code":12,
                 "msite":12,
                 "mcontent":12,
                 "attend_count":4,
                 "type":4,"status":4,
                 "address_id":4,
                 "email":12,
                 "contact_tel":12,
                 "create_user_name":12,
                 "create_user_id":12,
                 "creator_org":12,
                 "mstart_date":93,
                 "mend_date":93,
                 "create_time":93,
                 "update_user":12,
                 "update_time":93,
                 "company":4,
                 "sign_status":12
                },
      "table":"t_meeting_info",
      "ts":1595491574978,
      "type":"INSERT"
  }
  ```

- Update

  ```json
  {"data":[{
      "id":"18",
      "meeting_code":"M201907080071",
      "msite":"项目会议",
      "mcontent":"1",
      "attend_count":"5",
      "type":"1",
      "status":"5",
      "address_id":"72",
      "email":"*******",
      "contact_tel":"+86 *******",
      "create_user_name":"*******",
      "create_user_id":"*******",
      "creator_org":"*******",
      "mstart_date":"2019-07-20 08:30:00",
      "mend_date":"2019-07-20 18:00:00",
      "create_time":"2019-07-08 08:37:07",
      "update_user":null,
      "update_time":null,
      "company":"100",
      "sign_status":null}],
   "database":"canal_test",
   "es":1595492169000,
   "id":41368,
   "isDdl":false,
   "mysqlType":{    
       "id":"int(11)",         
       "meeting_code":"varchar(255)",
       "msite":"varchar(255)",
       "mcontent":"varchar(4096)",
       "attend_count":"int(5)",
       "type":"int(5)",
       "status":"int(255)",
       "address_id":"int(11)",
       "email":"varchar(255)",
       "contact_tel":"varchar(255)",
       "create_user_name":"varchar(255)",
       "create_user_id":"varchar(100)",
       "creator_org":"varchar(255)",
       "mstart_date":"datetime",
       "mend_date":"datetime",
       "create_time":"datetime",
       "update_user":"varchar(255)",
       "update_time":"datetime",
       "company":"int(10)",
       "sign_status":"varchar(255)"
    },
   "old":[{
       "mstart_date":"2019-07-19 08:30:00",
       "mend_date":"2019-07-19 18:00:00"}],
       "sql":"",
   "sqlType":{
       "id":4,"meeting_code":12,
       "msite":12,
       "mcontent":12,
       "attend_count":4,
       "type":4,
       "status":4,
       "address_id":4,
       "email":12,
       "contact_tel":12,
       "create_user_name":12,
       "create_user_id":12,
       "creator_org":12,
       "mstart_date":93,
       "mend_date":93,
       "create_time":93,
       "update_user":12,
       "update_time":93,
       "company":4,
       "sign_status":12},
   "table":"t_meeting_info",
   "ts":1595492169315,
   "type":"UPDATE"}
  ```

- Delete

  ```json
  {"data":[{
      "id":"18",
      "meeting_code":"M201907080071",
      "msite":"项目会议",
      "mcontent":"1",
      "attend_count":"5",
      "type":"1",
      "status":"5",
      "address_id":"72",
      "email":"*******",
      "contact_tel":"+86 *******",
      "create_user_name":"*******",
      "create_user_id":"*******",
      "creator_org":"*******",
      "mstart_date":"2019-07-20 08:30:00",
      "mend_date":"2019-07-20 18:00:00",
      "create_time":"2019-07-08 08:37:07",
      "update_user":null,
      "update_time":null,
      "company":"100",
      "sign_status":null
       }],
   "database":"canal_test",
   "es":1595492208000,
   "id":41372,
   "isDdl":false,
   "mysqlType":{
       "id":"int(11)",
       "meeting_code":"varchar(255)",
       "msite":"varchar(255)",
       "mcontent":"varchar(4096)",
       "attend_count":"int(5)",
       "type":"int(5)",
       "status":"int(255)",
       "address_id":"int(11)",
       "email":"varchar(255)",
       "contact_tel":"varchar(255)",
       "create_user_name":"varchar(255)",
       "create_user_id":"varchar(100)",
       "creator_org":"varchar(255)",
       "mstart_date":"datetime",
       "mend_date":"datetime",
       "create_time":"datetime",
       "update_user":"varchar(255)",
       "update_time":"datetime",
       "company":"int(10)",
       "sign_status":"varchar(255)"
   },
   "old":null,
   "sql":"",
   "sqlType":{
       "id":4,
       "meeting_code":12,
       "msite":12,
       "mcontent":12,
       "attend_count":4,
       "type":4,
       "status":4,
       "address_id":4,
       "email":12,
       "contact_tel":12,
       "create_user_name":12,
       "create_user_id":12,
       "creator_org":12,
       "mstart_date":93,
       "mend_date":93,
       "create_time":93,
       "update_user":12,
       "update_time":93,
       "company":4,
       "sign_status":12
   },
   "table":"t_meeting_info",
   "ts":1595492208356,
   "type":"DELETE"}
  ```

**`Json`格式解释**

- **data**：最新的数据，为JSON数组，如果是插入则表示最新插入的数据；如果是更新，则表示更新后的最新数据；如果是删除，则表示被删除的数据
- **database**：数据库名称
- **es**：事件时间，13位的时间戳
- **id**:事件操作的序列号，1，2,3
- **isDdl**：是否是DDL操作
- **mysql Type**：字段类型
- **old**：旧数据
- **pkNames**：主键名称
- **sql**：SQL语句
- **sqlType**：经过Canal转换处理的，unsigned int 会被转化为Long，unsigned long会被转换为BigDecimal
- **table**：表名
- **ts**:日志时间
- **type**:操作类型，例如DELETE、UPDATE、INSERT

**解析代码**

需要从info这个表里取：`id(int)`,`meeting_code(varchar)`,`address_id(int)`,`mstart_date(datetime)`,`mend_date(datetime)`

地区维表：

```sql
SELECT tma.id AS meetingroom_id,tma.name as meetingroom_name,tma.location as location_id,tml.full_name as location_name,tmr.`name` AS city
FROM t_meeting_address as tma 
LEFT JOIN t_meeting_location AS tml
ON tma.location=tml.code 
LEFT JOIN t_meeting_region AS tmr ON tml.region_id=tmr.id 
```

```java
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
```

#### 2.2.3 Flink计算逻辑

```java
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
        //dataStream.addSink(new SinkMeetingToMySQL()); //测试ok
        //dataStream.addSink(new SinkToMySQL());//测试ok
        dataStream.addSink(new SinkToGreenplum());//测试ok
        //执行
        env.execute("Meeting Streaming job");
    }
}
```

### 2.3 将结果写入数据库

![img](%E4%BC%9A%E8%AE%AE%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E5%AE%9E%E6%97%B6%E5%88%86%E6%9E%90%E6%80%BB%E7%BB%93.assets/raHGlh.jpg)

 **sink** 的意思也不一定非得说成要把数据存储到某个地方去。其实官网用的 **Connector** 来形容要去的地方更合适，这个 Connector 可以有 MySQL、ElasticSearch、Kafka、Cassandra RabbitMQ 、HDFS等，请看下面这张图片：

![img](%E4%BC%9A%E8%AE%AE%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F%E5%AE%9E%E6%97%B6%E5%88%86%E6%9E%90%E6%80%BB%E7%BB%93.assets/siWsAK.jpg)

#### 2.3.1 SinkToMysql

```java
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
 * sinktoMysql
 * 另外一种实现方法
 * */

public class SinkToMySQL extends RichSinkFunction<Meeting>{
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
        String sql="replace into meeting_result(meeting_id, meeting_code, meetingroom_id,meetingroom_name,location_name,city) values(?, ?, ?,?,?,?);";
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
    }

    private static  Connection getConnection(BasicDataSource dataSource) {
        Properties mysqlprop=new Properties();
        try {
            mysqlprop.load(new FileInputStream("D:\\flink\\src\\main\\java\\com\\sinks\\database.properties"));
            String mysqldriver=mysqlprop.getProperty("mysql_driver");
            String mysqlurl=mysqlprop.getProperty("mysql_url");
            String mysqlusername=mysqlprop.getProperty("mysql_Username");
            String mysqlpassword=mysqlprop.getProperty("mysql_Password");

            dataSource.setDriverClassName(mysqldriver);
            dataSource.setUrl(mysqlurl);
            dataSource.setUsername(mysqlusername);
            dataSource.setPassword(mysqlpassword);
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
            System.out.println("-----------mysql get connection has exception,msg=" +e.getMessage());
        }
        return con;
    }
}
```

#### 2.3.2 SinkToGreenplum

```java
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
```

## 3.可视化方案

- Tableau实时刷新Greenplum，FineBI也可以（秒级）
- DataV也可以每几秒刷新一次
- Flink计算后的结果，写入到缓存，前端开发可视化组件进行展示（实时展示）。

## 4.项目地址

https://github.com/liwei199411/FlinkStreamETL/tree/master

## 5.参考目录

[1].[基于Spark Streaming + Canal + Kafka对Mysql增量数据实时进行监测分析](https://www.cnblogs.com/itboys/p/10624670.html)

[2].[Canal](https://github.com/alibaba/canal)

[3].[Canal 的 .NET 客户端](https://github.com/dotnetcore/CanalSharp)

[4].[如何基于`MYSQL`做实时计算？](https://www.jianshu.com/p/19ab2cd28c63)

[5].[基于Canal与`Flink`实现数据实时增量同步(一)](https://jiamaoxiang.top/2020/03/05/基于Canal与Flink实现数据实时增量同步-一/)

[6].[美团DB数据同步到数据仓库的架构与实践](https://tech.meituan.com/2018/12/06/binlog-dw.html)

[7].[处理`JSON`格式的日志数据，然后进行流式Join](https://github.com/linweijiang/Flink-Demo/blob/master/src/main/java/utils/BinLogUtil.java)

[8].[`Flink`继续实践：从日志清洗到实时统计内容`PV`等多个指标](https://www.jianshu.com/p/52787491ea23)

[9].[实时数据架构体系建设思路](https://dbaplus.cn/news-73-3184-1.html)

[10].[Flink` 流与维表的关联]((https://liurio.github.io/2020/03/28/Flink流与维表的关联/))

[11].[**Flink DataStream`流表与维表Join(`Async` I/O)**]((https://blog.csdn.net/wangpei1949/article/details/96634493))

**12. `flink 流表join mysql表**

作者：`岳过山丘`
链接：https://www.jianshu.com/p/44583b98ecbb

**13. `flink1.9 使用LookupableTableSource实现异步维表关联**

作者：`todd5167`
链接：https://www.jianshu.com/p/7ebe1ec8aa7c

**14. `Flink异步之矛盾-锋利的Async I/O`**

作者：`王知无`
链接：https://www.jianshu.com/p/85ee258aa41f

**15.`Flink 的时间属性及原理解析`**

https://blog.csdn.net/zhengzhaoyang122/article/details/107352934?utm_medium=distribute.pc_relevant.none-task-blog-baidujs-3

**16.`大屏数据可视化`**
https://yyhsong.github.io/iDataV/







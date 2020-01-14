## Flink SQL 实战 (5)：SQL Client入门实践

本篇博客记录基于Flink 1.9.1发行版的SQL Client入门实践

在此入门实践中你可以学到：

- 搭建Flink、Kafka生产环境
- 使用Flink SQL查询Kafka Source Table

SQL Client本身无需过多介绍，详情可以参考[官方文档]( https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sqlClient.html )

我认为SQL Client入门的主要难点是搭建运行环境

## 搭建运行环境

因为SQL Client的启动脚本.sh文件只能在linux\Mac环境使用，windows系统用git bash也是不能运行的。

笔者使用一台 2核 4GB的云服务器，使用新安装的CentOS 7.6公共系统镜像进行操作：

#### 配置Java环境

我使用的是传输压缩包手动配置/etc/profile的方式配置

验证：

```
[root@Young ~]# java -version
java version "1.8.0_231"
Java(TM) SE Runtime Environment (build 1.8.0_231-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.231-b11, mixed mode)
```

#### 配置Kafka环境

- [下载](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.4.0/kafka_2.12-2.4.0.tgz) kafka 2.4.0 发行版 然后解压

```
wget http://mirror.bit.edu.cn/apache/kafka/2.4.0/kafka_2.12-2.4.0.tgz
```

```
tar -xzf kafka_2.12-2.4.0.tgz
```

如果机器内存不足可以通过配置server.properties的内存配置减少内存占用

- 运行zookeeper和kafka，在后台运行:

```
bin/zookeeper-server-start.sh config/zookeeper.properties &
```

```
bin/kafka-server-start.sh config/server.properties &
```

- 创建名为`log`的topic

```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic log
```

#### 配置Flink环境

- 下载 [Apache Flink 1.9.1 for Scala 2.11](https://www.apache.org/dyn/closer.lua/flink/flink-1.9.1/flink-1.9.1-bin-scala_2.11.tgz) 

```
wget http://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.9.1/flink-1.9.1-bin-scala_2.11.tgz
```

- 解压

```
tar -xzf flink-1.9.1-bin-scala_2.11.tgz
```

- 在lib文件夹下下载依赖`flink-json-1.9.1.jar`和`flink-sql-connector-kafka_2.11-1.9.1.jar` 

在lib文件夹下放多余的jar包在运行SQL Client时也会引发错误

```
wget http://central.maven.org/maven2/org/apache/flink/flink-json/1.9.1/flink-json-1.9.1.jar
```

```
wget http://central.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.9.1/flink-sql-connector-kafka_2.11-1.9.1.jar 
```

此时lib应该有如下依赖：

```
flink-dist_2.11-1.9.1.jar  flink-sql-connector-kafka_2.11-1.9.1.jar  flink-table-blink_2.11-1.9.1.jar  slf4j-log4j12-1.7.15.jar
flink-json-1.9.1.jar       flink-table_2.11-1.9.1.jar                log4j-1.2.17.jar
```

- (可选)配置TaskSlots数量

  编辑`conf/flink-conf.yaml`

  找到：taskmanager.numberOfTaskSlots，默认值为1，配置值为机器CPU实际核心数

  ```
  taskmanager.numberOfTaskSlots: 2
  ```

- 配置SQL配置文件

  在启动SQL Client时可以指定配置文件，如果不指定会默认读取 `conf/sql-client-defaults.yaml` 

  直接编辑 `conf/sql-client-defaults.yaml`  **修改 tables: []** 为：

  ```
  tables:
    - name: Logs
      type: source
      update-mode: append
      schema:
      - name: response
        type: STRING
      - name: status
        type: INT
      - name: protocol
        type: STRING
      - name: timestamp
        type: BIGINT
      connector:
        property-version: 1
        type: kafka
        version: universal
        topic: log
        startup-mode: earliest-offset
        properties:
        - key: zookeeper.connect
          value: 0.0.0.0:2181
        - key: bootstrap.servers
          value: 0.0.0.0:9092
        - key: group.id
          value: test
      format:
        property-version: 1
        type: json
        schema: "ROW(response STRING,status INT,protocol STRING,timestamp BIGINT)"
  ```

  以上配置描述了一个以JSON为数据源的Kafka tabele source，其格式同上篇博客使用的JSON格式

- 启动单机的Flink引擎

  ```
  ./bin/start-cluster.sh
  ```

  用浏览器访问8081端口：  http://服务器地址:8081，查看Flink控制面板

  主要看Available Task Slots的数量，如果为0说明没有计算资源无法正常执行计算任务，需要排查几种情况：

  - java版本是否为1.8.x ？（太新也不行）
  - 机器内存、CPU是否足够用？

#### 运行Flink SQL Client

- 以默认配置文件启动Flink SQL Client，会读取`conf/sql-client-defaults.yaml` 和`/lib`下的jar包，并进行验证、加载和构造类，完成后可以看到醒目的界面：

```bash
./bin/start-cluster.sh
```

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200114164027339.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzM1ODE1NTI3,size_16,color_FFFFFF,t_70)

#### CLI

- Hello World

  ```
  Flink SQL> SELECT 'Hello World';
  ```

   这个查询不需要表源，只产生一行结果。将会进入查询结果可视化界面。可以通过按下Q键来关闭结果视图。

- 查看表

  ```
  Flink SQL> SHOW TABLES;  
  Logs
  ```

   可以使用`SHOW TABLES`命令列出所有可用的表。将列出Source表、Sink表和视图。 

- 查看表结构

  ```
  Flink SQL> DESCRIBE Logs;
  root
   |-- response: STRING
   |-- status: INT
   |-- protocol: STRING
   |-- timestamp: BIGINT
  ```

   可以使用`DESCRIBE`命令查看表的结构 。

-  查看表中的数据 

  ```
  Flink SQL> SELECT * FROM Logs;
  ```

  执行`SELECT`语句，CLI将进入结果可视化模式并显示`Logs`表中的数据。 

  ![在这里插入图片描述](https://img-blog.csdnimg.cn/20200114170547270.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzM1ODE1NTI3,size_16,color_FFFFFF,t_70)

  在可视化界面可以看到启动了一个计算任务并占用了一个Task Slot

- 往Kafka打入数据

  稍微使用之前的测试数据脚本

  ```
  import pickle
  import time
  import json
  from kafka import KafkaProducer
  
  producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                           key_serializer=lambda k: pickle.dumps(k),
                           value_serializer=lambda v: pickle.dumps(v))
  start_time = time.time()
  for i in range(0, 10000):
      print('------{}---------'.format(i))
      producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),compression_type='gzip')
      producer.send('log',{"response":"res","status":0,"protocol":"protocol","timestamp":0})
      producer.send('log',{"response":"res","status":1,"protocol":"protocol","timestamp":0})
      producer.send('log',{"response":"resKEY","status":2,"protocol":"protocol","timestamp":0})
      producer.send('log',{"response":"res","status":3,"protocol":"protocol","timestamp":0})
      producer.send('log',{"response":"res","status":4,"protocol":"protocol","timestamp":0})
      producer.send('log',{"response":"res","status":5,"protocol":"protocol","timestamp":0})
  producer.flush()
  producer.close()
  end_time = time.time()
  time_counts = end_time - start_time
  print(time_counts)
  ```

  CentOS自带Python2环境，但使用此脚本需要提前安装Python Kafka依赖：

  ```
  pip install kafka-python
  ```

  执行脚本：

  ```
  python kafka_result.py
  ```

  我们只需要一丁点数据进行验证，执行一会儿就可以用 Shift + C 停止了

  查看CLI可视化查询结果界面：

  ![在这里插入图片描述](https://img-blog.csdnimg.cn/20200114170502840.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzM1ODE1NTI3,size_16,color_FFFFFF,t_70)

  大功告成，可以看到打到Kafka中的数据原原本本的显示出来

  根据下方提示，`+ -` 控制刷新速度 ,`N P` 上下翻页......都可以按一下熟悉操作

- 取消任务

  按`Q`取消任务，或者在可视化控制面板中点击该任务，点击`Cancel Job`来取消这个取消任务。

- 退出SQL Client

  ```
  Flink SQL> quit;
  ```

#### 

## GitHub

项目源码、博客.md文件、python小程序、添加依赖的jar包已上传至GitHub

 https://github.com/StarPlatinumStudio/Flink-SQL-Practice 

我的专栏：[Flink SQL原理和实战]( https://blog.csdn.net/qq_35815527/category_9634641.html )

### To Be Continue=>
## 实战篇-3：Tabel API & SQL 注册Tabel Source

在上一篇实战博客，我们给WordWithCount计算任务自定义了Sink函数

现在我们开始研究自定义Source:

### 前 方 干 货 极 多 ###



## 注册Tabel Source

我们以Kafka Source举例，讲2种注册Tabel Source的方法和一些技巧：

### 将DataStream转换为表

想要将DataStream转换为表，我们需要一个DataStream

以Kafka为外部数据源，需要在pom文件中添加依赖

```
<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-kafka-0.11_2.11</artifactId>
			<version>${flink.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-connector-kafka_2.11</artifactId>
			<version>${flink.version}</version>
		</dependency>
```

添加Kafka DataStream:

```
        DataStream<Tuple1<String>> kafkaStream=blinkStreamEnv.addSource(new FlinkKafkaConsumer011<>("test",new AbstractDeserializationSchema<Tuple1<String>>(){
            @Override
            public Tuple1<String> deserialize(byte[] bytes) throws IOException {
                return new Tuple1<>(new String(bytes,"utf-8"));
            }
        },kafkaProperties));
```

注册表:

```
//如果多列应为：fromDataStream(kafkaStream,"f0,f1,f2");
        Table source=blinkStreamTabelEnv.fromDataStream(kafkaStream,"word");
        blinkStreamTabelEnv.registerTable("kafkaDataStream",source);
```

虽然没有指定是Tabel Source，但是可以在后续流程使用注册好的 kafkaDataStream 表

### 数据类型到表架构的映射

Flink的DataStream和DataSet API支持非常多种类型。元组，POJO，Scala案例类和Flink的Row类型等复合类型允许嵌套的数据结构具有多个字段，这些字段可在表表达式中访问。

上述符合数据类型可以通过自定义Schema来使用

### 自定义Schema

我喜欢将自定义函数封装成类，简洁可复用

```
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.types.Row;
import java.io.IOException;

public final class RowSchema extends AbstractDeserializationSchema<Row> {
    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        //定义长度为1行的Row
        Row row=new Row(1);
        //设置字段，如果多行可以解析JSON循环
        row.setField(0,new String(bytes,"utf-8"));
        return row;
    }
}
```

在main中使用：

```
DataStream<Row> kafkaStream=blinkStreamEnv.addSource(new FlinkKafkaConsumer011<>("test",new RowSchema(),kafkaProperties));
```

到这里已经注册好可用的Datastream Source Tabel了

但是还可以进一步自定义：

### 自定义TableSource

StreamTableSource接口继承自TableSource接口，可以在getDataStream方法中编写DataStream

```
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

public class KafkaTabelSource implements StreamTableSource<String> {
    @Override
    public DataType getProducedDataType() {
        return DataTypes.STRING();
    }
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(new String[]{"word"},new DataType[]{DataTypes.STRING()}).build();
    }
    @Override
    public DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        Properties kafkaProperties=new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        kafkaProperties.setProperty("group.id", "test");
        DataStream<String> kafkaStream=env.addSource(new FlinkKafkaConsumer011<>("test",new SimpleStringSchema(),kafkaProperties));
        return kafkaStream;
    }
}
```

使用：

```
import kmops.Custom.KafkaTabelSource;
import kmops.Custom.MyRetractStreamTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
public class KafkaSource2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        blinkStreamTabelEnv.registerTableSource("kafkaDataStream",new KafkaTabelSource());//使用自定义TableSource
        RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(new String[]{"_count","word"},new DataType[]{DataTypes.BIGINT(), DataTypes.STRING()});
        blinkStreamTabelEnv.registerTableSink("sinkTable",retractStreamTableSink);
        Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT count(word) AS _count,word FROM kafkaDataStream GROUP BY word ");
        wordWithCount.insertInto("sinkTable");
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}
```

相当简洁就完成了自定义的Source、Sink

### Moke Kafka数据

有必要分享一下开发环境下kafka的使用：

入门请移步官网

 http://kafka.apache.org/quickstart 

### 使用Python Moke测试数据

 安装Python环境，pip kafka-python依赖，可以编写如下程序发送大量消息给Kafka:

```
# https://pypi.org/project/kafka-python/
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
    producer = KafkaProducer()
    producer.send('test', b'Xiaomi')
    producer.send('test', b'Xiaomi')
    producer.send('test', b'Xiaomi')
    producer.send('test', b'Apple')
    producer.send('test', b'Apple')
    producer.send('test', b'Huawei')
#    future = producer.send('test', key='num', value=i, partition=0)
# 将缓冲区的全部消息push到broker当中
producer.flush()
producer.close()

end_time = time.time()
time_counts = end_time -
```

输出结果：

```
26,Xiaomi
18,Apple
27,Xiaomi
28,Xiaomi
19,Apple
10,Huawei
29,Xiaomi
20,Apple
30,Xiaomi
21,Apple
11,Huawei
31,Xiaomi
22,Apple
32,Xiaomi
33,Xiaomi
12,Huawei
23,Apple
34,Xiaomi
35,Xiaomi
24,Apple
36,Xiaomi
```



### GitHub

源码、python kafka\socket moke小程序已上传至GitHub

 https://github.com/StarPlatinumStudio/Flink-SQL-Practice 



### To Be Continue=>
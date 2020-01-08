## Flink SQL 实战 (4)：UDF-用户自定义函数

在上一篇实战博客中分享了如自定义Schema这样实战中常用的code，

之前示例的WordWithCount只有可怜的一个字段不能算作典型，理解起来容易困惑，所有我们升级一下使用多个字段的JSON作为数据源:

```
{
	"response": "",
	"status": 0,
	"protocol": ""
	"timestamp":0
}
```

# 练习

根据之前实战篇的经验，创建一个 Tabel API/SQL 流处理项目

这次处理的数据源提高为来自Kafka的JSON数据

我使用转为 JavaBean 的方法处理数据源，首先写一个 JavaBean 类(构造函数、get()、set()可自动生成)

```
/**
 * JavaBean类
 * JSON:
 * {
 * 	"response": "",
 * 	"status": 0,
 * 	"protocol": ""
 * 	"timestamp":0
 * }
 */
public class Response {
    private String response;
    private int status;
    private String protocol;
    private long timestamp;

    public Response(String response, int status, String protocol, long timestamp) {
        this.response = response;
        this.status = status;
        this.protocol = protocol;
        this.timestamp = timestamp;
    }
    public Response(){}

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
```

要将 String 转义为 JavaBean 可以用 fastJson 实现

```
 <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.58</version>
        </dependency>
```

#### 自定义POJO Schema

Flink在类型之间进行了以下区分：

- 基本类型：所有的Java原语及其盒装形式，`void`，`String`，`Date`，`BigDecimal`，和`BigInteger`。
- 基本数组和对象数组
- 复合类型
  - Flink Java  `Tuples` （Flink Java API的一部分）：最多25个字段，不支持空字段
  - Scala  *case* 类（包括Scala元组）：不支持空字段
  - Row：具有任意多个字段并支持空字段的元组
  - POJO：遵循某种类似于bean的模式的类
- 辅助类型（ Option, Either, Lists, Maps,）
- 泛型类型：这些不会由Flink本身进行序列化，而是由Kryo进行序列化。

JSON转义为bean的模式属于POJO

```
public final class POJOSchema extends AbstractDeserializationSchema<Response> {
    @Override
    public Response deserialize(byte[] bytes) throws IOException {
        //byte[]转JavaBean
        try {
            return JSON.parseObject(bytes,Response.class);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }
}
```

#### 自定义TabelSource

```
public class KafkaTabelSource implements StreamTableSource<Response> {
    @Override
    public TypeInformation<Response> getReturnType() {
        return TypeInformation.of(Response.class);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(new String[]{"response","status","protocol","timestamp"},new DataType[]{DataTypes.STRING(),DataTypes.INT(),DataTypes.STRING(),DataTypes.BIGINT()}).build();
    }

    @Override
    public DataStream<Response> getDataStream(StreamExecutionEnvironment env) {
        Properties kafkaProperties=new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        kafkaProperties.setProperty("group.id", "test");
        DataStream<Response> kafkaStream=env.addSource(new FlinkKafkaConsumer011<>("test",new POJOSchema(),kafkaProperties));
        return kafkaStream;
    }
}
```

到这里使用之前编写的Sink已经可以简单运行 SELECT * FROM kafkaDataStream 查看效果

#### 试运行

编写Python脚本

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
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),compression_type='gzip')
    producer.send('test',{"response":"res","status":0,"protocol":"protocol","timestamp":0})
    producer.send('test',{"response":"res","status":1,"protocol":"protocol","timestamp":0})
    producer.send('test',{"response":"res","status":2,"protocol":"protocol","timestamp":0})
    producer.send('test',{"response":"res","status":3,"protocol":"protocol","timestamp":0})
    producer.send('test',{"response":"res","status":4,"protocol":"protocol","timestamp":0})
    producer.send('test',{"response":"res","status":5,"protocol":"protocol","timestamp":0})
#    future = producer.send('test', key='num', value=i, partition=0)
# 将缓冲区的全部消息push到broker当中
producer.flush()
producer.close()

end_time = time.time()
time_counts = end_time - start_time
print(time_counts)
```

main函数

```
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        KafkaTabelSource kafkaTabelSource=new KafkaTabelSource();
        blinkStreamTabelEnv.registerTableSource("kafkaDataStream",kafkaTabelSource);//使用自定义TableSource
        RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(kafkaTabelSource.getTableSchema().getFieldNames(),kafkaTabelSource.getTableSchema().getFieldDataTypes());
         blinkStreamTabelEnv.registerTableSink("sinkTable",retractStreamTableSink);
         Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT * FROM kafkaDataStream");
         wordWithCount.insertInto("sinkTable");
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
```

此时的运行结果是这样的：

```
res,1,protocol,0
res,2,protocol,0
res,3,protocol,0
res,4,protocol,0
res,5,protocol,0
res,0,protocol,0
res,1,protocol,0
res,2,protocol,0
res,3,protocol,0
res,4,protocol,0
res,5,protocol,0
res,0,protocol,0
res,1,protocol,0
......
```

## 用户自定义函数

用户自定义函数是一项重要功能，因为它们显着扩展了查询的表达能力。



在大多数情况下，必须先注册用户自定义函数，然后才能在查询中使用该函数。无需注册Scala Table API的函数。

`TableEnvironment`通过调用`registerFunction()`方法注册用户自定义函数。注册用户定义的函数后，会将其插入`TableEnvironment`的  catalog 中，以便Table API或SQL解析器可以识别并正确转义它。

[^Catalogs]: Catalogs 提供 metadata（元数据）,如databases, tables, partitions, views, 和 functions 还有访问存储在数据库或其他外部系统中的数据所需的信息。

### Scalar Functions(标量函数)

 如果内置函数中未包含所需的 scalar function ，就需要自定义一个 scalar function，自定义 scalar function 是 Table API 和 SQL 通用的。 自定义的 scalar function 将零个，一个或多个 scalar 值映射到新的 scalar 值。

 为了定义标量函数，必须扩展 ScalarFunction 并实现（一个或多个）评估方法。 标量函数的行为由评估方法确定。 评估方法必须公开声明并命名为 **eval** 。 评估方法的参数类型和返回类型也确定标量函数的参数和返回类型。 评估方法也可以通过实现多种名为eval的方法来重载。 

### 过滤Status的函数

自定义一个 ScalarFunction (UDF)，这个UDF的作用是简单判断参数 status 是否等于构造时指定的数字 

```
import org.apache.flink.table.functions.ScalarFunction;

public class IsStatus extends ScalarFunction {
    private int status = 0;
    public IsStatus(int status){
        this.status = status;
    }

    public boolean eval(int status){
        if (this.status == status){
            return true;
        } else {
            return false;
        }
    }
}
```

#### 注册UDF

注册`IsStatusFive`函数：判断参数是否等于5

```
blinkStreamTabelEnv.registerFunction("IsStatusFive",new IsStatus(5));
```

#### 编写 SQL

```
Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT * FROM kafkaDataStream WHERE IsStatusFive(status)");
```

#### 运行程序

最终main函数如下：

```
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        KafkaTabelSource kafkaTabelSource=new KafkaTabelSource();
        blinkStreamTabelEnv.registerTableSource("kafkaDataStream",kafkaTabelSource);//使用自定义TableSource
        RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(kafkaTabelSource.getTableSchema().getFieldNames(),kafkaTabelSource.getTableSchema().getFieldDataTypes());
         blinkStreamTabelEnv.registerTableSink("sinkTable",retractStreamTableSink);
         blinkStreamTabelEnv.registerFunction("IsStatusFive",new IsStatus(5));
         Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT * FROM kafkaDataStream WHERE IsStatusFive(status)");
         wordWithCount.insertInto("sinkTable");
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
```

输出结果

```
res,5,protocol,0
res,5,protocol,0
res,5,protocol,0
res,5,protocol,0
res,5,protocol,0
```

## GitHub

项目源码、python kafka moke小程序已上传至GitHub

 https://github.com/StarPlatinumStudio/Flink-SQL-Practice 

我的专栏：[Flink SQL原理和实战]( https://blog.csdn.net/qq_35815527/category_9634641.html )

### To Be Continue=>
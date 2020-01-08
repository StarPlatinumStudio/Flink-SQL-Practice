## 实战篇-2：Tabel API & SQL 自定义 Sinks函数

### 引子：匪夷所思的Bool数据

在上一篇实战博客，我们使用Flink SQL API编写了一个基本的WordWithCount计算任务

我截取了一段控制台输出：

```
2> (true,1,Huawei)
5> (false,1,Vivo)
5> (true,2,Vivo)
2> (false,1,Huawei)
2> (true,2,Huawei)
3> (true,1,Xiaomi)
3> (false,1,Xiaomi)
3> (true,2,Xiaomi)
```

不难发现我们定义的表数据本应该是只有LONG和STRING两个字段，但是控制台直接输出Tabel的结果却多出一个BOOL类型的数据。而且同样计数值的数据会出现true和false各一次。

在官方文档关于[retractstreamtablesink]( https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sourceSinks.html#retractstreamtablesink )的介绍中， 该表数据将被转换为一个累加和收回消息流，这些消息被编码为Java的 ```Tuple2<Boolean,T>``` 类型。第一个字段是一个布尔标志，用于指示消息类型(true表示插入，false表示删除)。第二个字段才是sink的数据类型。 

所以在我们的WordWithCount计算中，执行的SQL语句对表的操作不是单纯insert插入，而是每执行一次sink都会在sink中执行 **删除旧数据** 和 **插入新数据** 两次操作。

----

用于Flink Tabel环境的自定义 Sources & Sinks函数和DataStream API思路是差不多的，如果有编写DataStream APISources & Sinks函数的经验，编写用于Flink Tabel环境的自定义函数是较容易理解和上手的。

### 定义TableSink

现在我们要给之前的WordWithCount计算任务添加一个自定义Sink

![在这里插入图片描述](https://img-blog.csdnimg.cn/2020010616243882.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzM1ODE1NTI3,size_16,color_FFFFFF,t_70)





flink.table.sinks提供了有三种继承 ```StreamTableSink``` 类的接口: 

- AppendStreamTableSink: 仅发出对表插入的更改
- RetractStreamTableSink：发出对表具有插入，更新和删除的更改 ,消息被编码为 ```Tuple2<Boolean,T>```
- UpsertStreamTableSink： 发出对表具有插入，更新和删除的更改 ，消息被编码为 ```Tuple2<Boolean,T> ```，表必须要有类似主键的唯一键值（ 使用setKeyFields(方法），不然会报错

因为在我们的WordWithCount计算中，执行的SQL语句对表的操作不是单纯insert插入，所以我们需要编写实现RetractStreamTableSink的用户自定义函数：

```
public class MyRetractStreamTableSink implements RetractStreamTableSink<Row> {
   private TableSchema tableSchema;
   //构造函数，储存TableSchema
   public MyRetractStreamTableSink(String[] fieldNames,TypeInformation[] typeInformations){
       this.tableSchema=new TableSchema(fieldNames,typeInformations);
   }
   //重载
    public MyRetractStreamTableSink(String[] fieldNames,DataType[] dataTypes){
        this.tableSchema=TableSchema.builder().fields(fieldNames,dataTypes).build();
    }
    //Table sink must implement a table schema.
    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
               //自定义Sink
                // f0==true :插入新数据
                // f0==false:删除旧数据
                if(value.f0){
                //可以写入MySQL、Kafka或者发HttpPost...根据具体情况开发
                    System.out.println(value.f1);
                }
            }
        });
    }
    
    //接口定义的方法
    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(tableSchema.getFieldTypes(),tableSchema.getFieldNames());
    }
    //接口定义的方法
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }
    //接口定义的方法
    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
    }
   
}
```

吐槽一下，目前使用1.9.0版本API，在注册source Tabel都用 ```TypeInformation[]``` 表示数据类型。

而在编写Sink时使用```TypeInformation[]```的方法都被@Deprecated，提供了Builder方法代替构造，使用```DataType[]``` 为 ```TableSchema.builder().fields``` 的参数表示数据类型，统一使用 ```TypeInformation[]``` 表示数据类型比较潇洒，当然使用 ```TableSchema.builder()```  方法有对空值的检查，更加***可靠***。

所以写了重载函数：我全都要

使用自定义Sink,直接用new定义Tabel的结构简化了代码:

```
  import kmops.models.MyRetractStreamTableSink;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class CustomSinkJob {
    public static void main(String[] args) throws Exception {
        //初始化Flink执行环境
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        //获取Resource路径
        String path= CustomSinkJob.class.getClassLoader().getResource("list.txt").getPath();

        //注册数据源
        TableSource fileSource=new CsvTableSource(path,new String[]{"word"},new TypeInformation[]{Types.STRING});
        blinkStreamTabelEnv.registerTableSource("flieSourceTable",fileSource);

       //注册数据汇(Sink)
        RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(new String[]{"_count","word"},new TypeInformation[]{Types.LONG,Types.STRING});
        //或者
        //RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(new String[]{"_count","word"},new DataType[]{DataTypes.BIGINT(),DataTypes.STRING()});
        blinkStreamTabelEnv.registerTableSink("sinkTable",retractStreamTableSink);

        //执行SQL
        Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT count(word) AS _count,word FROM flieSourceTable GROUP BY word ");

        //将SQL结果插入到Sink Table
        wordWithCount.insertInto("sinkTable");
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

```

输出结果：

```
1,OnePlus
1,Oppo
2,Oppo
2,OnePlus
```

### GitHub

源码已上传至GitHub

 https://github.com/StarPlatinumStudio/Flink-SQL-Practice 

下篇博客干货极多

### To Be Continue=>
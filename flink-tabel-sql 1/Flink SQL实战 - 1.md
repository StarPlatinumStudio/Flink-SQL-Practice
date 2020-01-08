## 实战篇-0

# Apache Flink® SQL Training

### 创建Blink流式查询项目

#### 新建MAVEN Java模板

可以在命令行使用maven也可以通过IDEA快速创建flink job模板

这里使用的是1.9.0版本的flink

```
 $ mvn archetype:generate                               \
      -DarchetypeGroupId=org.apache.flink              \
      -DarchetypeArtifactId=flink-quickstart-java      \
      -DarchetypeVersion=1.9.0
```

```
工程cmd中的树结构
D:\Flink\flink-tabel-sql>tree /f
卷 Document 的文件夹 PATH 列表
卷序列号为 B412-6CDC
D:.
│  flink-tabel-sql.iml
│  pom.xml
│
├─.idea
│      compiler.xml
│      encodings.xml
│      misc.xml
│      workspace.xml
│
├─src
│  └─main
│      ├─java
│      │  └─kmops
│      │          BatchJob.java
│      │          StreamingJob.java
│      │
│      └─resources
│              log4j.properties
│
└─target
    ├─classes
    │  │  log4j.properties
    │  │
    │  └─kmops
    │          BatchJob.class
    │          StreamingJob.class
    │
    └─generated-sources
        └─annotations
```

在pom.xml中添加dependcy

- 使用Java编程语言支持流/批的Table＆SQL API。 

- 支持国产，这里选择阿里贡献的Blink planner,

  **注意**: Blink可能不适用1.9.0以前的flink

```
<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-api-java-bridge_2.11</artifactId>
			<version>1.9.0</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-planner-blink_2.11</artifactId>
			<version>1.9.0</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-streaming-scala_2.11</artifactId>
			<version>1.9.0</version>
		</dependency>
		<dependency>
			<groupId>org.apache.flink</groupId>
			<artifactId>flink-table-common</artifactId>
			<version>1.9.0</version>
		</dependency>
```

创建一个Blink流式查询任务（BLINK STREAMING QUERY）

```
public class TabelJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        //TODO
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}
```

一个Blink流式查询任务的模板就写好啦

上述代码使用StreamTableEnvironment的create()方法，以StreamExecutionEnvironment、EnvironmentSettings为参数创建了一个StreamTableEnvironment，同样的，对EnvironmentSettings调用函数稍加修改我们就可以创建[blink批处理查询任务、兼容旧版的流/批任务]( https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/common.html )。

现在我们使用Tabel API/SQL编写一个WordWithCount程序，

## 在 Catalog 中注册表格

#### 连接外部系统

自Flink1.6以来，连接外部系统的声明是和其实际实现隔离的。

- 既可以用 Table API & SQL 以编程的方式实现

- 也可以使用YAML配置文件在SQL Client上完成

   这不仅可以更好地统一API和SQL Client，还可以在自定义实现的情况下更好地扩展而不更改实际声明。 

   每个声明都类似于SQL CREATE TABLE语句。 可以定义表的名称，表的架构，连接器以及用于连接到外部系统的数据格式。  连接器描述存储表数据的外部系统。 

我们在这里以编程的方式注册表格:

这里列举2种注册表格的写法

#### 1.定义TableSource注册表格

```
		String path=TabelJob.class.getClassLoader().getResource("list.txt").getPath();
		String[] fieldNames={"word"};
        TypeInformation[] fieldTypes={Types.STRING};
        TableSource fileSource=new CsvTableSource(path,fieldNames,fieldTypes);
        blinkStreamTabelEnv.registerTableSource("FlieSourceTable",fileSource);
```

  #### 2.使用connect方法注册表格

  以下示例包含了编程链接外部系统的流程

这里参考了GitHub [hequn8128](https://github.com/hequn8128)的Tabel API DEMO

https://github.com/hequn8128/TableApiDemo 

![alt 属性文本](https://ss3.bdstatic.com/70cFv8Sh_Q1YnxGkpoWK1HF6hhy/it/u=1640722149,1219744404&fm=15&gp=0.jpg "可选标题")

- Connectors：连接器，连接数据源，读取文件使用FileSystem()

- Formats:数据源数据格式，在官方的格式表中，读取文件属于 Old CSV (for files) 

- Table Schem：定义列的名称和类型，类似于SQL `CREATE TABLE`语句的列定义。 

- 更新模式 Update Modes

  .inAppendMode()： 在 Append 模式下，动态表和外部连接器只交换插入消息。 

  除此之外还有[Retract\Upsert Mode]( https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/connect.html#update-modes )

  **注意**：每个连接器的文档都说明了支持哪些更新模式

```
    String path=TabelJob.class.getClassLoader().getResource("list.txt").getPath();
    blinkStreamTabelEnv
                .connect(new FileSystem().path(path))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word",Types.STRING))
                .inAppendMode()
                .registerTableSource("FlieSourceTable");
```

**注意:** 在Flink中，要获取数据源就需要连接外部系统，不同的数据格式请[参考]( https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/connect.html#file-system-connector )。

### 表

在 Resource 目录下创建名为"list.txt"的文件，写入几行文字

```
Apple
Xiaomi
Huawei
Oppo
...
```

现在我们注册好了一个名为“FlieSourceTable”的表，根据我们的定义和文件内容，结构如下：

| word:String |
| ---- |
| Apple |
| Xiaomi |
|...... |

## 查询表

我们要计算 fileSource 表中的word字段中各个单词的数量

查询表也有2种方式: Table API 和 SQL

### Table API

```
Table result = tEnv.scan("fileSource")
			.groupBy("word")
			.select("word, count(word) as _count");
```

### SQL

 Flink使用支持标准ANSI SQL的[Apache Calcite](https://calcite.apache.org/docs/reference.html)解析SQL。不支持DDL语句。 

**注意** Flink SQL解析有非常多保留关键字所有在给行命名时要留意命名，可以添加下划线解决尴尬。

```
Table wordWithCount = blinkStreamTabelEnv
.sqlQuery("SELECT count(word) AS _count,word FROM FlieSourceTable GROUP BY word");
```

查询完成生成的wordWithCount表，因为数据源时是无界的流数据，

所以最新的结果的 _count 字段是根据历史累计计数不断增加的：

| word:String | _count:? |
| ---- | ---- |
| Apple |1|
| Xiaomi |1|
| Xiaomi |2|
|Apple |2|
|Apple |3|
|... |...|

### 输出

如果只想简单调试程序可以直接在控制台打印table的内容

```
        blinkStreamTabelEnv.toRetractStream(wordWithCount, Row.class).print();
        blinkStreamEnv.execute("BLINK STREAMING QUERY");
```

查看控制台输出，可以发现 _count 字段在不断累加：

```
3> (false,Xiaomi,1)
5> (true,Apple,2)
15:48:27,773 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutor            - Un-registering task and sending final execution state FINISHED to JobManager for task CsvTableSource(read fields: word) -> SourceConversion(table=[default_catalog.default_database.FlieSourceTable, source: [CsvTableSource(read fields: word)]], fields=[word]) 692f7d7611e92283f458ee0ef0cd4034.
3> (true,Xiaomi,2)
15:48:27,789 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - CsvTableSource(read fields: word) -> SourceConversion(table=[default_catalog.default_database.FlieSourceTable, source: [CsvTableSource(read fields: word)]], fields=[word]) (1/8) (692f7d7611e92283f458ee0ef0cd4034) switched from RUNNING to FINISHED.
3> (false,Xiaomi,2)
3> (true,Xiaomi,3)
5> (false,Apple,2)
5> (true,Apple,3)
```

#### 项目源码：

 https://github.com/StarPlatinumStudio/Flink-SQL-Practice 

不断更新中。。。



在2019的最后一天，祝大家新年快乐

下一章节：注册TableSink

### To Be Continue=>
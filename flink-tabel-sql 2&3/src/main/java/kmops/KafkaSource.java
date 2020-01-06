package kmops;
import kmops.Custom.MyRetractStreamTableSink;
import kmops.Custom.RowSchema;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws Exception {
        //初始化Flink执行环境
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);

        Properties kafkaProperties=new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        kafkaProperties.setProperty("group.id", "test");
        DataStream<Row> kafkaStream=blinkStreamEnv.addSource(new FlinkKafkaConsumer011<>("test",new RowSchema(),kafkaProperties));
//        DataStream<Tuple1<String>> kafkaStream=blinkStreamEnv.addSource(new FlinkKafkaConsumer011<>("test",new AbstractDeserializationSchema<Tuple1<String>>(){
//            @Override
//            public Tuple1<String> deserialize(byte[] bytes) throws IOException {
//                return new Tuple1<>(new String(bytes,"utf-8"));
//            }
//        },kafkaProperties));

        //如果多列应为：fromDataStream(kafkaStream,"f0,f1,f2");
        Table source=blinkStreamTabelEnv.fromDataStream(kafkaStream,"word");
        blinkStreamTabelEnv.registerTable("kafkaDataStream",source);

       //注册数据汇(Sink)
        RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(new String[]{"_count","word"},new DataType[]{DataTypes.BIGINT(), DataTypes.STRING()});
        blinkStreamTabelEnv.registerTableSink("sinkTable",retractStreamTableSink);

        //执行SQL
        Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT count(word) AS _count,word FROM kafkaDataStream GROUP BY word ");

        //将SQL结果插入到Sink Table
        wordWithCount.insertInto("sinkTable");
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

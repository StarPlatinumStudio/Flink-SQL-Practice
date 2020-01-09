package robinwang;
import robinwang.custom.KafkaTabelSource;
import robinwang.custom.MyRetractStreamTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.types.Row;
import robinwang.udfs.IsStatus;

/**
 *  查看kafkaDataStream中status=5的数据
 */
public class UdsfJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        KafkaTabelSource kafkaTabelSource=new KafkaTabelSource();
        blinkStreamTabelEnv.registerTableSource("kafkaDataStream",kafkaTabelSource);//使用自定义TableSource
//        RetractStreamTableSink<Row> retractStreamTableSink=new MyRetractStreamTableSink(kafkaTabelSource.getTableSchema().getFieldNames(),kafkaTabelSource.getTableSchema().getFieldDataTypes());
//         blinkStreamTabelEnv.registerTableSink("sinkTable",retractStreamTableSink);
         blinkStreamTabelEnv.registerFunction("IsStatusFive",new IsStatus(5));
         Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT * FROM kafkaDataStream WHERE IsStatusFive(status)");
        blinkStreamTabelEnv.toAppendStream(wordWithCount,Row.class).print();
       //  wordWithCount.insertInto("sinkTable");
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

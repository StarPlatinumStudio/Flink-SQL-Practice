package robinwang;
import robinwang.custom.KafkaTabelSource;
import robinwang.custom.MyRetractStreamTableSink;
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

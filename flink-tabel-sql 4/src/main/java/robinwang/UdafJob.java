package robinwang;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import robinwang.custom.KafkaTabelSource;
import robinwang.udfs.KyeWordCount;
import robinwang.udfs.MaxStatus;

/**
 *聚合最大的status
 */
public class UdafJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTabelEnv = StreamTableEnvironment.create(streamEnv, streamSettings);
        KafkaTabelSource kafkaTabelSource = new KafkaTabelSource();
        streamTabelEnv.registerTableSource("kafkaDataStream", kafkaTabelSource);//使用自定义TableSource
        streamTabelEnv.registerFunction("maxStatus",new MaxStatus());
        Table wordWithCount = streamTabelEnv.sqlQuery("SELECT maxStatus(status) AS maxStatus FROM kafkaDataStream");
        streamTabelEnv.toRetractStream(wordWithCount, Row.class).print();
        streamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

package robinwang;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import robinwang.custom.KafkaTabelSource;
import robinwang.udfs.KyeWordCount;
/**
 * 关键字过滤统计
 */
public class UdtfJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment streamTabelEnv = StreamTableEnvironment.create(streamEnv, streamSettings);
        KafkaTabelSource kafkaTabelSource = new KafkaTabelSource();
        streamTabelEnv.registerTableSource("kafkaDataStream", kafkaTabelSource);//使用自定义TableSource
        streamTabelEnv.registerFunction("CountKEY", new KyeWordCount(new String[]{"KeyWord","WARNING","illegal"}));
        Table wordWithCount = streamTabelEnv.sqlQuery("SELECT key,COUNT(countv) AS countsum FROM kafkaDataStream LEFT JOIN LATERAL TABLE(CountKEY(response)) as T(key, countv) ON TRUE GROUP BY key");
        streamTabelEnv.toRetractStream(wordWithCount, Row.class).print();
        streamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

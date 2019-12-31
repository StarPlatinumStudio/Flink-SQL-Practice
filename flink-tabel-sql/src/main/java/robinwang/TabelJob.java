package robinwang;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class TabelJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        String path=TabelJob.class.getClassLoader().getResource("list.txt").getPath();
        blinkStreamTabelEnv
                .connect(new FileSystem().path(path))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word",Types.STRING))
                .inAppendMode()
                .registerTableSource("FlieSourceTable");

        Table wordWithCount = blinkStreamTabelEnv.scan("FlieSourceTable")
                .groupBy("word")
                .select("word,count(word) as _count");
        blinkStreamTabelEnv.toRetractStream(wordWithCount, Row.class).print();
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

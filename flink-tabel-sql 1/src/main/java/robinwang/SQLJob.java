package robinwang;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class SQLJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment blinkStreamEnv=StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings blinkStreamSettings= EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamTabelEnv= StreamTableEnvironment.create(blinkStreamEnv,blinkStreamSettings);
        String path= SQLJob.class.getClassLoader().getResource("list.txt").getPath();
        String[] fieldNames={"word"};
        TypeInformation[] fieldTypes={Types.STRING};
        TableSource fileSource=new CsvTableSource(path,fieldNames,fieldTypes);
        blinkStreamTabelEnv.registerTableSource("FlieSourceTable",fileSource);
        Table wordWithCount = blinkStreamTabelEnv.sqlQuery("SELECT count(word) AS _count,word FROM FlieSourceTable GROUP BY word");
        blinkStreamTabelEnv.toRetractStream(wordWithCount, Row.class).print();
        blinkStreamTabelEnv.execute("BLINK STREAMING QUERY");
    }
}

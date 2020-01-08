package robinwang;
import robinwang.custom.MyRetractStreamTableSink;
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

package robinwang.custom;

import robinwang.entity.Response;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Properties;
/**
 * {
 * 	"response": "",
 * 	"status": 0,
 * 	"protocol": ""
 * 	"timestamp":0
 * }
 */
public class KafkaTabelSource implements StreamTableSource<Response> {
    @Override
    public TypeInformation<Response> getReturnType() {
//        对于非泛型类型，传递Class
        return TypeInformation.of(Response.class);
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(new String[]{"response","status","protocol","timestamp"},new DataType[]{DataTypes.STRING(),DataTypes.INT(),DataTypes.STRING(),DataTypes.BIGINT()}).build();
    }

    @Override
    public DataStream<Response> getDataStream(StreamExecutionEnvironment env) {
        Properties kafkaProperties=new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        kafkaProperties.setProperty("group.id", "test");
        DataStream<Response> kafkaStream=env.addSource(new FlinkKafkaConsumer011<>("test",new POJOSchema(),kafkaProperties));
        return kafkaStream;
    }
}

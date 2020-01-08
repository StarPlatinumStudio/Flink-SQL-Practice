package robinwang.custom;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

public class KafkaTabelSource implements StreamTableSource<String> {
    @Override
    public DataType getProducedDataType() {
        return DataTypes.STRING();
    }
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(new String[]{"word"},new DataType[]{DataTypes.STRING()}).build();
    }
    @Override
    public DataStream<String> getDataStream(StreamExecutionEnvironment env) {
        Properties kafkaProperties=new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        kafkaProperties.setProperty("group.id", "test");
        DataStream<String> kafkaStream=env.addSource(new FlinkKafkaConsumer011<>("test",new SimpleStringSchema(),kafkaProperties));
        return kafkaStream;
    }
}

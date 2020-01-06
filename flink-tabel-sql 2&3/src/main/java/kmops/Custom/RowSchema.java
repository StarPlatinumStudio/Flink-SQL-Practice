package kmops.Custom;


import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.types.Row;
import java.io.IOException;

public final class RowSchema extends AbstractDeserializationSchema<Row> {
    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        //定义长度为1行的Row
        Row row=new Row(1);
        //设置字段，如果多行可以解析JSON循环
        row.setField(0,new String(bytes,"utf-8"));
        return row;
    }
}

package robinwang.custom;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple1;

import java.io.IOException;

public final class Tuple1Schema extends AbstractDeserializationSchema<Tuple1<String>> {
    @Override
    public Tuple1<String> deserialize(byte[] bytes) throws IOException {
        return new Tuple1<>(new String(bytes,"utf-8"));
    }
}

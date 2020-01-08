package robinwang.custom;

import com.alibaba.fastjson.JSON;
import robinwang.entity.Response;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * JSON:
 * {
 * 	"response": "",
 * 	"status": 0,
 * 	"protocol": ""
 * 	"timestamp":0
 * }
 */
public final class POJOSchema extends AbstractDeserializationSchema<Response> {
    @Override
    public Response deserialize(byte[] bytes) throws IOException {
        //byte[]转JavaBean
        try {
            return JSON.parseObject(bytes,Response.class);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
        return null;
    }
}

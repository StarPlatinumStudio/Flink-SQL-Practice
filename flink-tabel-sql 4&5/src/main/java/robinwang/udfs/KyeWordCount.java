package robinwang.udfs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

public class KyeWordCount extends TableFunction<Tuple2<String,Integer>> {
    private String[] keys;
    public KyeWordCount(String[] keys){
        this.keys=keys;
    }
    public void eval(String in){
        for (String key:keys){
            if (in.contains(key)){
                collect(new Tuple2<String, Integer>(key,1));
            }
        }
    }
}

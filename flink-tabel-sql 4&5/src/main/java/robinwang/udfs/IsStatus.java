package robinwang.udfs;

import org.apache.flink.table.functions.ScalarFunction;

public class IsStatus extends ScalarFunction {
    private int status = 0;
    public IsStatus(int status){
        this.status = status;
    }

    public boolean eval(int status){
        if (this.status == status){
            return true;
        } else {
            return false;
        }
    }
}

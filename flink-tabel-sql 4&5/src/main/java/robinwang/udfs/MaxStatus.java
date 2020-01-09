package robinwang.udfs;

import org.apache.flink.table.functions.AggregateFunction;

public class MaxStatus extends AggregateFunction<Integer,MaxStatus.StatusACC> {
    @Override
    public Integer getValue(StatusACC statusACC) {
        return statusACC.maxStatus;
    }

    @Override
    public StatusACC createAccumulator() {
        return new StatusACC();
    }
    public void accumulate(StatusACC statusACC,int status){
        if (status>statusACC.maxStatus){
            statusACC.maxStatus=status;
        }
    }
    public static class StatusACC{
        public int maxStatus=0;
    }
}

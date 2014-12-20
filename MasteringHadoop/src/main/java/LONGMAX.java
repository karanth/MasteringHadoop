package MasteringHadoop;

import org.apache.pig.Accumulator;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Iterator;

public class LONGMAX extends EvalFunc<Long> implements Accumulator<Long> {

    private Long intermediateMax = null;

    @Override
    public Long exec(Tuple objects) throws IOException {
        return max(objects);
    }


    @Override
    public void accumulate(Tuple objects) throws IOException {
        Long newIntermediateMax = max(objects);

        if(newIntermediateMax == null){
            return;
        }

        if(intermediateMax == null){
            intermediateMax = Long.MIN_VALUE;
        }

        intermediateMax = Math.max(intermediateMax, newIntermediateMax);

     }

    @Override
    public Long getValue() {
        return intermediateMax;
    }

    @Override
    public void cleanup() {
        intermediateMax = null;
    }

    protected static Long max(Tuple input) throws ExecException{
        long returnMax = Long.MIN_VALUE;
        DataBag dataBag = (DataBag) input.get(0);
        for(Iterator<Tuple> it = dataBag.iterator(); it.hasNext();){
            Tuple tuple = it.next();
            Long currentValue = (Long)tuple.get(0);
            if(currentValue > returnMax){
                returnMax = currentValue;
            }
        }
        return returnMax;

    }

}

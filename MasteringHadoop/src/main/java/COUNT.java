package MasteringHadoop;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.Iterator;

public class COUNT extends EvalFunc<Long> implements Algebraic {



     protected static Long count(Tuple input) throws ExecException{
         DataBag dataBag = (DataBag) input.get(0);
         return new Long(dataBag.size());
    }

    protected static Long sum(Tuple input) throws ExecException{

        Long returnSum = new Long(0);
        DataBag dataBag = (DataBag) input.get(0);
        for(Iterator<Tuple> it = dataBag.iterator(); it.hasNext();){
            Tuple tuple = it.next();
            returnSum += (Long)tuple.get(0);
        }
        return returnSum;

    }

    static class Initial extends EvalFunc<Long>{

        @Override
        public Long exec(Tuple objects) throws IOException {
            return count(objects);  //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    static class Intermediate extends EvalFunc<Long>{


        @Override
        public Long exec(Tuple objects) throws IOException {
            return sum(objects);  //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    static class Final extends EvalFunc<Long>{

        @Override
        public Long exec(Tuple objects) throws IOException {
            return sum(objects);  //To change body of implemented methods use File | Settings | File Templates.
        }
    }


    @Override
    public Long exec(Tuple objects) throws IOException {
        return count(objects);  //To change body of implemented methods use File | Settings | File Templates.
    }


    @Override
    public String getInitial() {
        return Initial.class.getName();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getIntermed() {
        return Intermediate.class.getName();  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getFinal() {
        return Final.class.getName();  //To change body of implemented methods use File | Settings | File Templates.
    }
}

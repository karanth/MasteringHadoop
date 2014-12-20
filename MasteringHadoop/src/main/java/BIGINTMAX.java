/*package MasteringHadoop;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.LongWritable;


public class BIGINTMAX extends UDAF {

    public static class MaximumBigIntEvaluator implements UDAFEvaluator{

        private Long max;
        private boolean empty;


        public MaximumBigIntEvaluator(){
            super();
            init();
        }


        @Override
        public void init() {
            max = (long)0;
            empty = true;
        }


        public boolean iterate(LongWritable value){
            if(value != null){

                long current = value.get();
                if(empty){
                    max = current;
                    empty = false;

                }
                else{

                    max = Math.max(current, max);
                }


            }
            return true;


        }


        public LongWritable terminatePartial(){
            return empty ? null : new LongWritable(max);

        }

        public LongWritable terminate(){
            return empty ? null : new LongWritable(max);

        }

        public boolean merge(LongWritable value){
          iterate(value);
          return true;
        }

    }

}
     */
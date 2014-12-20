package MasteringHadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;


public class CsvLoader extends LoadFunc {
    private RecordReader recordReader = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private static byte DELIMITER = (byte)',';
    private ArrayList<Object> tupleArrayList = null;


    @Override
    public void setLocation(String s, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, s);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }


    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.recordReader = recordReader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try{

            if(recordReader.nextKeyValue()){

                Text value  = (Text) recordReader.getCurrentValue();
                byte[] buffer = value.getBytes();

                tupleArrayList = new ArrayList<Object>();

                int start = 0;
                int i = 0;
                int len = value.getLength();

                while(i < len){

                    if(buffer[i] == DELIMITER){

                        readFields(buffer, start, i);
                        start = i + 1;

                    }
                    i++;
                }

               readFields(buffer, start, len);

               Tuple returnTuple = tupleFactory.newTupleNoCopy(tupleArrayList);
               tupleArrayList = null;

               return returnTuple;


            }

        }
        catch(InterruptedException ex){
            //Error handling

        }
        return null;


    }


    private void readFields(byte[] buffer, int start, int i){
        if(start == i){
            //Null field
            tupleArrayList.add(null);
        }
        else{
            //Read from start to i
            tupleArrayList.add(new DataByteArray(buffer, start, i));
        }

    }


}

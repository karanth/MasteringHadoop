package MasteringHadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeJoinKeyWritable implements WritableComparable<CompositeJoinKeyWritable> {

    private Text key = new Text();
    private IntWritable source = new IntWritable();

    public CompositeJoinKeyWritable(){

    }

    public CompositeJoinKeyWritable(String key, int source){

        this.key.set(key);
        this.source.set(source);

    }

    public IntWritable getSource(){
        return this.source;
    }

    public Text getKey(){
        return this.key;
    }

    public void setSource(int source){
        this.source.set(source);
    }

    public void setKey(String key){
        this.key.set(key);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        this.key.write(dataOutput);
        this.source.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.key.readFields(dataInput);
        this.source.readFields(dataInput);

    }


    @Override
    public int compareTo(CompositeJoinKeyWritable o) {

        int result = this.key.compareTo(o.key);

        if(result == 0){
            return this.source.compareTo(o.source);
        }


        return result;
    }

    @Override
    public boolean equals(Object obj){

        if(obj instanceof CompositeJoinKeyWritable){

            CompositeJoinKeyWritable joinKeyWritable = (CompositeJoinKeyWritable)obj;

            return (key.equals(joinKeyWritable.key) && source.equals(joinKeyWritable.source));
        }

        return false;

    }
}

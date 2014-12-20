package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import MasteringHadoop.CompositeJoinKeyWritable;

public class MasteringHadoopReduceSideJoin {

    public static class MasteringHadoopReduceSideJoinCountryMap extends Mapper<LongWritable, Text, CompositeJoinKeyWritable, Text>{

        private static short COUNTRY_CODE_INDEX = 0;
        private static short COUNTRY_NAME_INDEX = 1;

        private  CompositeJoinKeyWritable joinKeyWritable = new CompositeJoinKeyWritable("", 1);
        private  Text recordValue = new Text("");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",", -1);

            if(tokens != null){
                recordValue.set(tokens[COUNTRY_NAME_INDEX]);
                joinKeyWritable.setKey(tokens[COUNTRY_CODE_INDEX]);
                context.write(joinKeyWritable, recordValue);
            }


        }
    }

    public static class MasteringHadoopReduceSideJoinCityMap extends Mapper<LongWritable, Text, CompositeJoinKeyWritable, Text>{

        private static short COUNTRY_CODE_INDEX = 0;


        private  CompositeJoinKeyWritable joinKeyWritable = new CompositeJoinKeyWritable("", 2);
        private  Text record = new Text("");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",", -1);

            if(tokens != null){
                joinKeyWritable.setKey(tokens[COUNTRY_CODE_INDEX]);
                record.set(value.toString());
                context.write(joinKeyWritable, record);
            }


        }
    }




    public static class MasteringHadoopReduceSideJoinReduce extends
            Reducer<CompositeJoinKeyWritable, Text, Text, LongWritable>{

        private  LongWritable populationValue = new LongWritable(0);
        private  Text countryValue = new Text("");
        private static short POPULATION_INDEX = 4;

        @Override
        protected void reduce(CompositeJoinKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            long populationTotal = 0;
            boolean firstRecord = true;
            String country = null;
            for(Text record : values){

                String[] tokens = record.toString().split(",", -1);


                if(firstRecord == true){
                    firstRecord = false;

                    if(tokens.length > 1){
                        break;
                    }else{
                      country = record.toString();
                    }
                }
                else{
                    String populationString = tokens[POPULATION_INDEX];
                    if(populationString != null && populationString.isEmpty() == false){
                        populationTotal += Long.parseLong(populationString);
                    }
                }

            }

            if(country != null){
                populationValue.set(populationTotal);
                countryValue.set(country);
                context.write(countryValue, populationValue);

            }

        }
    }

    public static class CompositeJoinKeyPartitioner extends Partitioner<CompositeJoinKeyWritable, Text>{
      @Override
        public int getPartition(CompositeJoinKeyWritable key, Text value, int partitions) {

            return key.getKey().hashCode() % partitions;

        }
    }


    public static class CompositeJoinKeyGroupComparator extends WritableComparator{

        protected CompositeJoinKeyGroupComparator(){
             super(CompositeJoinKeyWritable.class, true);

        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {

            CompositeJoinKeyWritable compositeKey1 = (CompositeJoinKeyWritable) a;
            CompositeJoinKeyWritable compositeKey2 = (CompositeJoinKeyWritable) b;

            return compositeKey1.getKey().compareTo(compositeKey2.getKey());

        }
    }

   /* public static class CompositeJoinKeySortComparator extends WritableComparator{
        protected CompositeJoinKeySortComparator(){
            super(CompositeJoinKeyWritable.class, true);

        }

        @Override
        public int compare(Object a, Object b) {

            CompositeJoinKeyWritable compositeKey1 = (CompositeJoinKeyWritable) a;
            CompositeJoinKeyWritable compositeKey2 = (CompositeJoinKeyWritable) b;

            int result = compositeKey1.getKey().compareTo(compositeKey2.getKey());


            if(result == 0){

                return Integer.compare(compositeKey1.getSource(), compositeKey2.getSource());
            }

            return result;
        }
    }
     */


    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{

        GenericOptionsParser parser = new GenericOptionsParser(args);
        Configuration config = parser.getConfiguration();
        String[] remainingArgs = parser.getRemainingArgs();

        Job job = Job.getInstance(config, "MasteringHadoop-ReduceSideJoin");

        job.setMapOutputKeyClass(CompositeJoinKeyWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        job.setReducerClass(MasteringHadoopReduceSideJoinReduce.class);
        job.setPartitionerClass(CompositeJoinKeyPartitioner.class);
        //job.setSortComparatorClass(CompositeJoinKeySortComparator.class);
        job.setGroupingComparatorClass(CompositeJoinKeyGroupComparator.class);
        job.setNumReduceTasks(1);


        MultipleInputs.addInputPath(job, new Path(remainingArgs[0]), TextInputFormat.class, MasteringHadoopReduceSideJoinCountryMap.class);
        MultipleInputs.addInputPath(job, new Path(remainingArgs[1]), TextInputFormat.class, MasteringHadoopReduceSideJoinCityMap.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[2]));

        job.waitForCompletion(true);

    }



}

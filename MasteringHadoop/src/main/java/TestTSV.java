/**
 * Created with IntelliJ IDEA.
 * User: sandeepkaranth
 * Date: 08/04/14
 * Time: 11:41 AM
 * To change this template use File | Settings | File Templates.
 */

package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class TestTSV{

     public static class TSVMap extends  Mapper<LongWritable, Text, LongWritable, Text>{

         private static int ARTISTE_ID_FIELD = 2;
         private static int ARTISTE_NAME = 13;
         private static int FIELDS_TOTAL = 52;

         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String record = value.toString();

             //Don't truncate empty fields
             String[] tokens = record.split("\t", -1);

             if(tokens.length >= FIELDS_TOTAL){
                 int artisteId = Integer.parseInt(tokens[ARTISTE_ID_FIELD]);
                 String artisteName = tokens[ARTISTE_NAME];

                 context.write(key, new Text(record));
             }


         }
     }

     public static class TSVReduce extends Reducer<IntWritable, Text, IntWritable, Text>{





     }


    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration config = new Configuration();

        Job job = Job.getInstance(config, "TSVDump");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TSVMap.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));


        job.waitForCompletion(true);




    }

}

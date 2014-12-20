/**
 * Created with IntelliJ IDEA.
 * User: sandeepkaranth
 * Date: 09/04/14
 * Time: 9:17 PM
 * To change this template use File | Settings | File Templates.
 */

package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;

public class CombineFilesMasteringHadoop {


    public static class CombineFilesMapper extends  Mapper<LongWritable, Text, LongWritable, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
        GenericOptionsParser parser = new GenericOptionsParser(args);
        Configuration config = parser.getConfiguration();
        String[] remainingArgs = parser.getRemainingArgs();

        Job job = Job.getInstance(config, "MasteringHadoop-CombineDemo");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(CombineFilesMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(MasteringHadoop.MasteringHadoopCombineFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));


        job.waitForCompletion(true);
    }


}

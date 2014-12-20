
package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;




public class MasteringHadoopChainingJobs {

    public static class MasteringHadoopChainingJobsMap1 extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        private IntWritable countOfWords = new IntWritable(0);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            countOfWords.set(tokenizer.countTokens());
            context.write(key, countOfWords);
        }
    }


    public static class MasteringHadoopChainingJobsMap2 extends Mapper<LongWritable, IntWritable, LongWritable, IntWritable> {

        private int wordCountFilter = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);    //To change body of overridden methods use File | Settings | File Templates.
            String wordCountFilterString = context.getConfiguration().get("wordcount.filter");

            if(wordCountFilterString != null && !wordCountFilterString.isEmpty()){
                wordCountFilter = Integer.parseInt(wordCountFilterString);
            }
        }

        @Override
        protected void map(LongWritable key, IntWritable value, Context context) throws IOException, InterruptedException {

            if(value.get() > wordCountFilter)
                context.write(key, value);
        }
    }


    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{

        GenericOptionsParser parser = new GenericOptionsParser(args);
        Configuration config = parser.getConfiguration();
        String[] remainingArgs = parser.getRemainingArgs();

        Job job = Job.getInstance(config, "MasteringHadoop-ChainingJobs");
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);


        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        TextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        Configuration conf1 = new Configuration(false);
        ChainMapper.addMapper(job, MasteringHadoopChainingJobsMap1.class, LongWritable.class, Text.class, LongWritable.class, IntWritable.class, conf1);

        Configuration conf2 = new Configuration(false);
        ChainMapper.addMapper(job, MasteringHadoopChainingJobsMap2.class, LongWritable.class, IntWritable.class, LongWritable.class, IntWritable.class, conf2);

        job.waitForCompletion(true);

    }


}

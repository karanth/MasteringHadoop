package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class MasteringHadoopCounters {


        public static enum WORDS_IN_LINE_COUNTER{
            ZERO_WORDS,
            LESS_THAN_FIVE_WORDS,
            MORE_THAN_FIVE_WORDS
        };

        public static class MasteringHadoopCountersMap extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

            private IntWritable countOfWords = new IntWritable(0);

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                StringTokenizer tokenizer = new StringTokenizer(value.toString());
                int words = tokenizer.countTokens();

                if(words == 0)
                    context.getCounter(WORDS_IN_LINE_COUNTER.ZERO_WORDS).increment(1);
                if(words > 0 && words <= 5)
                    context.getCounter(WORDS_IN_LINE_COUNTER.LESS_THAN_FIVE_WORDS).increment(1);
                else
                    context.getCounter(WORDS_IN_LINE_COUNTER.MORE_THAN_FIVE_WORDS).increment(1);

                countOfWords.set(words);
                context.write(key, countOfWords);
            }
        }


        public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{

            GenericOptionsParser parser = new GenericOptionsParser(args);
            Configuration config = parser.getConfiguration();
            String[] remainingArgs = parser.getRemainingArgs();

            Job job = Job.getInstance(config, "MasteringHadoop-Counters");
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(MasteringHadoopCountersMap.class);

            job.setNumReduceTasks(0);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);


            TextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
            TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));


            job.waitForCompletion(true);


            Counters counters = job.getCounters();

            for(CounterGroup cg : counters){
                System.out.println("Counter Group: " + cg.getDisplayName() + " " + cg.getName());
                for(Counter c : cg){
                    System.out.println("Counter: " + c.getDisplayName() + " " + c.getName() + " " + c.getValue());
               }
            }


    }



}

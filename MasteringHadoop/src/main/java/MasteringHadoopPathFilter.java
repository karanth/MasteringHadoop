
package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: sandeepkaranth
 * Date: 10/04/14
 * Time: 7:37 PM
 * To change this template use File | Settings | File Templates.
 */



public class MasteringHadoopPathFilter {

    public static class MasteringHadoopPathFilterMap extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    public static class MasteringHadoopPathAndSizeFilter extends Configured implements PathFilter {

        private Configuration configuration;
        private Pattern filePattern;
        private long filterSize;
        private FileSystem fileSystem;

        @Override
        public boolean accept(Path path){

            boolean isFileAcceptable = true;

            try{
                if(fileSystem.isDirectory(path)){

                    return true;
                }

                if(filePattern != null){

                    Matcher m = filePattern.matcher(path.toString());
                    isFileAcceptable = m.matches();
                }

                if(filterSize > 0){
                    long actualFileSize = fileSystem.getFileStatus(path).getLen();
                    if(actualFileSize > this.filterSize){
                        isFileAcceptable &= true;
                    }
                    else{
                        isFileAcceptable = false;
                    }
                }

            }
            catch(IOException ioException){
                //Error handling goes here.

            }

            return isFileAcceptable;
        }

        @Override
        public void setConf(Configuration conf){
            this.configuration = conf;

            if(this.configuration != null){
                String filterRegex = this.configuration.get("filter.name");

                if(filterRegex != null){
                    this.filePattern = Pattern.compile(filterRegex);
                }

                String filterSizeString = this.configuration.get("filter.min.size");

                if(filterSizeString != null){
                    this.filterSize = Long.parseLong(filterSizeString);
                }

                try{
                    this.fileSystem = FileSystem.get(this.configuration);
                }
                catch(IOException ioException){
                    //Error handling
                }

            }
        }
    }


    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{

        GenericOptionsParser parser = new GenericOptionsParser(args);
        Configuration config = parser.getConfiguration();
        String[] remainingArgs = parser.getRemainingArgs();

        Job job = Job.getInstance(config, "MasteringHadoop-PathFilterDemo");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MasteringHadoopPathFilterMap.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPathFilter(job, MasteringHadoopPathAndSizeFilter.class);
        TextInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));


        job.waitForCompletion(true);

    }


}

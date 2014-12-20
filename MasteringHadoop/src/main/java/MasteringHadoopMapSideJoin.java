package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.TreeMap;

public class MasteringHadoopMapSideJoin {

    public static class MasteringHadoopMapSideJoinMap extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static short COUNTRY_CODE_INDEX = 0;
        private static short COUNTRY_NAME_INDEX = 1;
        private static short POPULATION_INDEX = 4;


        private TreeMap<String, String> countryCodesTreeMap = new TreeMap<String, String>();
        private Text countryKey = new Text("");
        private LongWritable populationValue = new LongWritable(0);


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            URI[] localFiles = context.getCacheFiles();

            String path = null;
            for(URI uri : localFiles){
                path = uri.getPath();
                if(path.trim().equals("countrycodes.txt")){
                     break;
                }

            }

            if(path != null){
               getCountryCodes(path, context);
            }

        }

        private void getCountryCodes(String path, Context context) throws IOException{

            Configuration configuration = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream in = fileSystem.open(new Path(path));
            Text line = new Text("");
            LineReader lineReader = new LineReader(in, configuration);

            int offset = 0;
            do{
                offset = lineReader.readLine(line);

                if(offset > 0){
                    String[] tokens = line.toString().split(",", -1);
                    countryCodesTreeMap.put(tokens[COUNTRY_CODE_INDEX], tokens[COUNTRY_NAME_INDEX]);
                }

            }while(offset != 0);


        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String cityRecord = value.toString();
            String[] tokens = cityRecord.split(",", -1);

            String country = tokens[COUNTRY_CODE_INDEX];
            String populationString = tokens[POPULATION_INDEX];

            if(country != null && country.isEmpty() == false){

                if(populationString != null && populationString.isEmpty() == false){

                    long population = Long.parseLong(populationString);
                    String countryName = countryCodesTreeMap.get(country);

                    if(countryName == null) countryName = country;

                    countryKey.set(countryName);
                    populationValue.set(population);
                    context.write(countryKey, populationValue);

                }

            }
       }
    }


    public static class MasteringHadoopMapSideJoinReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

        private static LongWritable populationValue = new LongWritable(0);
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

             long populationTotal = 0;

             for(LongWritable population : values){
                populationTotal += population.get();
             }
            populationValue.set(populationTotal);
            context.write(key, populationValue);
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{

        GenericOptionsParser parser = new GenericOptionsParser(args);
        Configuration config = parser.getConfiguration();
        String[] remainingArgs = parser.getRemainingArgs();


        Job job = Job.getInstance(config, "MasteringHadoop-MapSideJoin");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(MasteringHadoopMapSideJoinMap.class);
        job.setCombinerClass(MasteringHadoopMapSideJoinReduce.class);
        job.setReducerClass(MasteringHadoopMapSideJoinReduce.class);

        job.addCacheFile(new URI(remainingArgs[0]));
        job.setNumReduceTasks(3);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        TextInputFormat.addInputPath(job, new Path(remainingArgs[1]));
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[2]));

        job.waitForCompletion(true);

    }



}

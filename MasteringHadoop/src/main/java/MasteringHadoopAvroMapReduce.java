package MasteringHadoop;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class MasteringHadoopAvroMapReduce {

    private static String citySchema = "{\"namespace\": \"MasteringHadoop.avro\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"City\",\n" +
            " \"fields\": [\n" +
            "     {\"name\": \"countryCode\", \"type\": \"string\"},\n" +
            "     {\"name\": \"cityName\",  \"type\": \"string\"},\n" +
            "     {\"name\": \"cityFullName\", \"type\": \"string\"},\n" +
            "     {\"name\": \"regionCode\", \"type\": [\"int\",\"null\"]},\n" +
            "     {\"name\": \"population\", \"type\": [\"long\", \"null\"]},\n" +
            "     {\"name\": \"latitude\", \"type\": [\"float\", \"null\"]},\n" +
            "     {\"name\": \"longitude\", \"type\": [\"float\", \"null\"]}\n" +
            " ]\n" +
            "}";


    public static class MasteringHadoopAvroMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, LongWritable>{

        private Text ccode = new Text();
        private LongWritable population = new LongWritable();
        private String inputSchema;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            inputSchema = context.getConfiguration().get("citySchema");
         }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {

            GenericRecord record = key.datum();
            String countryCode = (String) record.get("countryCode");
            Long cityPopulation = (Long) record.get("population");

            if(cityPopulation != null){
                ccode.set(countryCode);
                population.set(cityPopulation.longValue());
                context.write(ccode, population);

            }

        }
    }



    public static class MasteringHadoopAvroReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

        private LongWritable total = new LongWritable();


        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long totalPopulation = 0;

            for(LongWritable pop : values){
                totalPopulation += pop.get();
            }

            total.set(totalPopulation);
            context.write(key, total);
        }
    }




    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{

        GenericOptionsParser parser = new GenericOptionsParser(args);
        Configuration config = parser.getConfiguration();
        String[] remainingArgs = parser.getRemainingArgs();

        config.set("citySchema", citySchema);

        Job job = Job.getInstance(config, "MasteringHadoop-AvroMapReduce");

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.addCacheFile(new URI(remainingArgs[2]));

        job.setMapperClass(MasteringHadoopAvroMapper.class);
        job.setReducerClass(MasteringHadoopAvroReducer.class);
        job.setNumReduceTasks(1);

        Schema schema  = (new Schema.Parser()).parse(new File(remainingArgs[2]));
        AvroJob.setInputKeySchema(job, schema);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        AvroKeyInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        job.waitForCompletion(true);

    }



}

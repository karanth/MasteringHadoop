package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

/*
public class MasteringHadoopSequenceFile {

    public static void writeSequenceFile(String textFile, String seqFile) throws IOException{

        Path readPath = new Path(textFile);
        Path writePath = new Path(seqFile);
        Configuration conf = new Configuration(false);
        FileSystem fs = FileSystem.get(URI.create(textFile), conf);
        BufferedReader bufferedReader = null;
        SequenceFile.Writer sequenceFileWriter = null;

        try{

            bufferedReader = new BufferedReader
                    (new InputStreamReader
                            (fs.open(readPath)));

            sequenceFileWriter = SequenceFile.createWriter(conf,
                                                            SequenceFile.Writer.file(writePath),
                                                            SequenceFile.Writer.keyClass(LongWritable.class),
                                                            SequenceFile.Writer.valueClass(Text.class));
            String line = null;
            LongWritable key = new LongWritable();
            Text value = new Text();
            long lineCount = 0;

            while((line = bufferedReader.readLine()) != null){
                key.set(lineCount);
                lineCount++;
                value.set(line);
                sequenceFileWriter.append(key, value);

            }


        }
        catch(IOException ioEx){
            ioEx.printStackTrace();
        }
        finally{
           if(sequenceFileWriter != null)
                sequenceFileWriter.close();

           if(bufferedReader != null)
                bufferedReader.close();
        }
   }

   public static void readSequenceFile(String seqFile) throws IOException{

       Path readPath = new Path(seqFile);
       Configuration conf = new Configuration(false);
       FileSystem fs = FileSystem.get(URI.create(seqFile), conf);

       SequenceFile.Reader reader = null;

       try{
         reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(readPath));
         Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
         Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);

         while(reader.next(key,value)){
             System.out.println("key: " + key.toString());
             if(reader.syncSeen()){
                 System.out.println("sync: ");
             }

         }
       }
       catch(IOException ioEx){
           ioEx.printStackTrace();
       }
       finally{
           if(reader !=  null){
            reader.close();
           }
       }

   }


    public static void main(String[] args){

       try{
        writeSequenceFile(args[0], args[1]);
        readSequenceFile(args[1]);
       }
       catch(IOException ioEx){
           ioEx.printStackTrace();
       }

    }

}
    */
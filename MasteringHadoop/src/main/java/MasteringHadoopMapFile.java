package MasteringHadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;
import java.net.URI;
/*
public class MasteringHadoopMapFile {

    public static void writeMapFile(String seqFile) throws IOException {

        Path readPath = new Path(seqFile);
        Path mapPath = new Path(readPath, MapFile.DATA_FILE_NAME);

        Configuration conf = new Configuration(false);
        FileSystem fs = FileSystem.get(URI.create(seqFile), conf);

        SequenceFile.Reader reader = null;

        try{
            reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(mapPath));
            Class keyClass = reader.getKeyClass();
            Class valueClass = reader.getValueClass();

            MapFile.fix(fs, readPath, keyClass, valueClass, false, conf);

        }
        catch(IOException ioEx){
            ioEx.printStackTrace();
        }
        catch(Exception ex){
            ex.printStackTrace();
        }
        finally{
            if(reader !=  null){
                reader.close();
            }
        }

    }


    public static void main(String[] args){

        try{
            writeMapFile(args[0]);

        }
        catch(IOException ioEx){
            ioEx.printStackTrace();
        }

    }



}
       */
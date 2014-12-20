package MasteringHadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class MasteringHadoopSerialization
{
    public static String serializeToByteString(Writable writable) throws IOException {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        writable.write(dataOutputStream);
        dataOutputStream.close();

        byte[] byteArray = outputStream.toByteArray();

        return StringUtils.byteToHexString(byteArray);
    }

    public static String javaSerializeToByteString(Object o) throws IOException{
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(o);
        objectOutputStream.close();

        byte[] byteArray = outputStream.toByteArray();

        return StringUtils.byteToHexString(byteArray);

    }

    public static void main(String[] args) throws IOException{

        IntWritable intWritable = new IntWritable();
        VIntWritable vIntWritable = new VIntWritable();
        LongWritable longWritable = new LongWritable();
        VLongWritable vLongWritable = new VLongWritable();

        int smallInt = 100;
        int mediumInt = 1048576;
        long bigInt = 4589938592L;

        System.out.println("smallInt serialized value using IntWritable");
        intWritable.set(smallInt);
        System.out.println(serializeToByteString(intWritable));

        System.out.println("smallInt serialized value using VIntWritable");
        vIntWritable.set(smallInt);
        System.out.println(serializeToByteString(vIntWritable));

        System.out.println("mediumInt serialized value using IntWritable");
        intWritable.set(mediumInt);
        System.out.println(serializeToByteString(intWritable));

        System.out.println("mediumInt serialized value using VIntWritable");
        vIntWritable.set(mediumInt);
        System.out.println(serializeToByteString(vIntWritable));

        System.out.println("bigInt serialized value using LongWritable");
        longWritable.set(bigInt);
        System.out.println(serializeToByteString(longWritable));

        System.out.println("mediumInt serialized value using VIntWritable");
        vLongWritable.set(bigInt);
        System.out.println(serializeToByteString(vLongWritable));


        System.out.println("smallInt serialized value using Java serializer");
        System.out.println(javaSerializeToByteString(new Integer(smallInt)));


        System.out.println("mediumInt serialized value using Java serializer");
        System.out.println(javaSerializeToByteString(new Integer(mediumInt)));

        System.out.println("bigInt serialized value using Java serializer");
        System.out.println(javaSerializeToByteString(new Long(bigInt)));



    }

}

package MasteringHadoop;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.*;


public class MasteringHadoopCsvToAvro {

   public static void CsvToAvro(String csvFilePath, String avroFilePath, String schemaFile) throws IOException{

        //Read the schema
        Schema schema  = (new Schema.Parser()).parse(new File(schemaFile));
        File avroFile = new File(avroFilePath);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema,avroFile);



        BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFilePath));
        String commaSeparatedLine;
        while((commaSeparatedLine = bufferedReader.readLine()) != null){

            GenericRecord city = getCountry(commaSeparatedLine, schema);
            if(city != null)
                dataFileWriter.append(city);
        }

        dataFileWriter.close();

    }

    private static GenericRecord getCountry(String commaSeparatedLine, Schema schema){
        GenericRecord country = null;
        String[] tokens = commaSeparatedLine.split(",");

        if(tokens.length == 2){
            country = new GenericData.Record(schema);
            country.put("countryCode", tokens[0]);
            country.put("countryName", tokens[1]);
        }

        return country;


    }

    private static GenericRecord getCity(String commaSeparatedLine, Schema schema){

         GenericRecord city = null;
         String[] tokens = commaSeparatedLine.split(",");

        //Filter out the bad tokens
         if(tokens.length == 7){
             city = new GenericData.Record(schema);
             city.put("countryCode", tokens[0]);
             city.put("cityName", tokens[1]);
             city.put("cityFullName", tokens[2]);

             if(tokens[3] != null && tokens[3].length() > 0 && isNumeric(tokens[3])){
                 city.put("regionCode", Integer.parseInt(tokens[3]));
             }


             if(tokens[4] != null && tokens[4].length() > 0 && isNumeric(tokens[4])){
                city.put("population", Long.parseLong(tokens[4]));
             }


             if(tokens[5] != null && tokens[5].length() > 0 && isNumeric(tokens[5])){
                 city.put("latitude", Float.parseFloat(tokens[5]));
             }

             if(tokens[6] != null && tokens[6].length() > 0 && isNumeric(tokens[6])){
                 city.put("longitude", Float.parseFloat(tokens[6]));
             }
         }

         return city;

         }


         public static void main(String[] args){

             try{
                CsvToAvro(args[0], args[1], args[2]);
             }
             catch(IOException iox){
                iox.printStackTrace();
             }

         }


    public static boolean isNumeric(String str)
    {
        try
        {
            double d = Double.parseDouble(str);
        }
        catch(NumberFormatException nfe)
        {
            return false;
        }
        return true;
    }

}

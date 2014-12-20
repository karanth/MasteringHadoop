/**
 * Created with IntelliJ IDEA.
 * User: sandeepkaranth
 * Date: 28/04/14
 * Time: 7:05 PM
 * To change this template use File | Settings | File Templates.
 */

package MasteringHadoop;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class UPPER extends EvalFunc<String>{


    @Override
    public String exec(Tuple objects) throws IOException {

       if(objects == null || objects.size() == 0){
           return null;
       }
       try{
           String inputString = (String) objects.get(0);
           return inputString.toUpperCase();
       }
       catch(Exception ex){

           throw new IOException("Error processing input ", ex);
       }

    }
}

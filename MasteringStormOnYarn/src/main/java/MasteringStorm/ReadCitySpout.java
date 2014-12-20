package MasteringStorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;


public class ReadCitySpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader cityFileReader;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
         this.spoutOutputCollector = spoutOutputCollector;

         try{
              cityFileReader = new BufferedReader(new FileReader((String)map.get("city.file")));
         }
         catch(Exception ex){
             ex.printStackTrace();
         }

    }

    @Override
    public void nextTuple() {

        String city = null;
        if(cityFileReader != null){

            try {
                city = cityFileReader.readLine();
            }
            catch(Exception ex){
                ex.printStackTrace();
            }

        }


        if(city != null){
            spoutOutputCollector.emit(new Values(city));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
          outputFieldsDeclarer.declare(new Fields("city"));
    }
}

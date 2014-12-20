package MasteringStorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by sandeepkaranth on 07/07/14.
 */
public class FilterCityBolt extends BaseRichBolt {

    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String city = tuple.getString(0);

        String[] tokens = city.split(",");

        //Filter cities that have a population number.
        if(tokens != null && tokens.length >= 7 && tokens[4] != null && tokens[4].length() > 0){

            try {
                Long population = Long.parseLong(tokens[4]);
            }
            catch(NumberFormatException ex){
                city = null;
            }

            if(city != null)
                collector.emit(new Values(tokens[0],city));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("countryCode","city"));

    }
}

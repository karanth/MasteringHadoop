package MasteringStorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;


public class SumPopulationForCountryBolt extends BaseRichBolt {

    private HashMap<String, Long> countryCodePopulationMap;
    private OutputCollector outputCollector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.outputCollector = outputCollector;
        this.countryCodePopulationMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String countryCode = tuple.getString(0);
        String city = tuple.getString(1);
        String[] tokens = city.split(",");
        Long population = Long.parseLong(tokens[4]);

        if(countryCodePopulationMap.containsKey(countryCode)) {
            Long savedPopulation = countryCodePopulationMap.get(tokens[0]);
            population += savedPopulation;
            countryCodePopulationMap.remove(countryCode);
        }

        countryCodePopulationMap.put(countryCode, population);
        outputCollector.emit(new Values(countryCode, population));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("countryCode", "population"));

    }
}

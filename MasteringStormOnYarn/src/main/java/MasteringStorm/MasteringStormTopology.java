package MasteringStorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MasteringStormTopology {

    public static void main(String[] args){

        Config config = new Config();
        config.setDebug(true);

        if(args != null){
             System.out.println(args.length);
        }

        System.out.println(args[0]);
        config.put("city.file", args[0]);

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("cities", new ReadCitySpout(), 3);
        topologyBuilder.setBolt("filter", new FilterCityBolt(), 3).shuffleGrouping("cities");
        topologyBuilder.setBolt("group", new SumPopulationForCountryBolt(), 3).fieldsGrouping("filter", new Fields("countryCode"));

        try {
            StormSubmitter.submitTopology("test-filtering-storm", config, topologyBuilder.createTopology());

        }
        catch(Exception ex){
            ex.printStackTrace();
        }

    }


}

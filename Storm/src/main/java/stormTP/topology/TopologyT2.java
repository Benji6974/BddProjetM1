package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import stormTP.operator.ExitBolt;
import stormTP.operator.StreamSimSpout;
import stormTP.operator.bolt.GivePointsBolt;

public class TopologyT2 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = 9001;
		int portOUTPUT = 9002;
		String ipmINPUT = "224.0.0." + args[0];
		String ipmOUTPUT = "225.0.0." + args[0];
    	
		/*Création du spout*/
    	StreamSimSpout spout = new StreamSimSpout(portINPUT, ipmINPUT);
    	/*Création de la topologie*/
    	TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("localStream", spout);
        /*Affectation à la topologie du bolt qui ne fait rien, il prendra en input le spout localStream*/
        builder.setBolt("GivePointsBolt", new GivePointsBolt(), nbExecutors).shuffleGrouping("localStream");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new ExitBolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("GivePointsBolt");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*Affectation de workers pour la topologie */
        config.setNumWorkers(1);
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT2", config, builder.createTopology());
	}
		
	
}
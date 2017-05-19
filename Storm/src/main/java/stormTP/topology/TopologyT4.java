package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import stormTP.operator.ExitBolt;
import stormTP.operator.StreamSimSpout;
import stormTP.operator.bolt.AnomalyDetectionBolt;

public class TopologyT4 {
	
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
        builder.setBolt("AnomalyDetectionBolt", new AnomalyDetectionBolt().withWindow(new BaseWindowedBolt.Count(2)), nbExecutors).shuffleGrouping("localStream");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new ExitBolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("AnomalyDetectionBolt");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*Affectation de workers pour la topologie */
        config.setNumWorkers(1);
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT4", config, builder.createTopology());
	}
		
	
}
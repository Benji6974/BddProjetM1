package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import stormTP.operator.ExitBolt;
import stormTP.operator.StreamSimSpout;
import stormTP.operator.bolt.CheckPointsBolt;
import stormTP.operator.bolt.GivePointsBoltBis;

public class TopologyT7 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = 9001;
		int portINPUT2 = 9002;
		int portOUTPUT = 9002;
		String ipmINPUT = "224.0.0." + args[0];
		String ipmINPUT2 = "225.0.0." + args[0];
		String ipmOUTPUT = "225.0.0." + args[1];
    	
		/*Création du spout*/

		StreamSimSpout spout = new StreamSimSpout(portINPUT, ipmINPUT);
		StreamSimSpout spout2 = new StreamSimSpout(portINPUT, ipmINPUT2);
    	/*Création de la topologie*/
    	TopologyBuilder builder = new TopologyBuilder();
        /*Affectation à la topologie du spout*/
        builder.setSpout("localStream", spout);
		builder.setSpout("localStream2", spout2);
        /*Affectation à la topologie du bolt qui ne fait rien, il prendra en input le spout localStream*/
        builder.setBolt("GivePointsBoltBis", new GivePointsBoltBis(), nbExecutors)
				.shuffleGrouping("localStream");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
		builder.setBolt("CheckPointsBolt", new CheckPointsBolt()
				.withMessageIdField("id")
				.withWindow(new BaseWindowedBolt.Count(2)), nbExecutors)
				.shuffleGrouping("GivePointsBoltBis")
				.shuffleGrouping("localStream2");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new ExitBolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("CheckPointsBolt");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*Affectation de workers pour la topologie */
        config.setNumWorkers(1);
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT7", config, builder.createTopology());
	}
		
	
}
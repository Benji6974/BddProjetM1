package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import stormTP.operator.ExitBolt;
import stormTP.operator.StreamSimSpout;
import stormTP.operator.bolt.GivePointsBoltBis;
import stormTP.operator.bolt.PodiumBolt;

import java.util.concurrent.TimeUnit;

public class TopologyT6 {
	
	public static void main(String[] args) throws Exception {
		int nbExecutors = 1;
		int portINPUT = 9001;
		int portOUTPUT = 9002;

        String ipmOUTPUT = "225.0.0." + args[0];
        /*Création de la topologie*/
        TopologyBuilder builder = new TopologyBuilder();

        for(int i=142; i<170; i++){
            /*Création du spout*/
            StreamSimSpout spout = new StreamSimSpout(portINPUT, "224.0.0."+i);
            /*Affectation à la topologie du spout*/
            builder.setSpout("localStream"+i, spout);
            /*Affectation à la topologie du bolt qui ne fait rien, il prendra en input le spout localStream*/

        }
        BoltDeclarer b = builder.setBolt("GivePointsBoltBis", new GivePointsBoltBis(), nbExecutors);
        for(int i=142; i<170; i++){
            b.shuffleGrouping("localStream"+i);
        }
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
		builder.setBolt("PodiumBolt", new PodiumBolt()
				.withMessageIdField("id")
				.withTumblingWindow(new BaseWindowedBolt.Duration(15, TimeUnit.SECONDS)), nbExecutors)
				.shuffleGrouping("GivePointsBoltBis");
        /*Affectation à la topologie du bolt qui émet le flux de sortie, il prendra en input le bolt nofilter*/
        builder.setBolt("exit", new ExitBolt(portOUTPUT, ipmOUTPUT), nbExecutors).shuffleGrouping("PodiumBolt");
       
        /*Création d'une configuration*/
        Config config = new Config();
        /*Affectation de workers pour la topologie */
        config.setNumWorkers(1);
        /*La topologie est soumise à STORM*/
        StormSubmitter.submitTopology("topoT6", config, builder.createTopology());
	}
		
	
}
/**
 * 
 */
package stormTP.operator;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * @author Lumineau
 *
 */

public class BigDataSpout implements IRichSpout {

	private static final long serialVersionUID = -299357684149378960L;
	private static Logger logger = Logger.getLogger("BigDataSpout");
	private SpoutOutputCollector collector;
	private Random rand;
	private long msgId = 0;
//	private long nbT = 1;
    private long initTimestamp = 0; 
    
	
    public BigDataSpout(long initts){
    	this.initTimestamp = initts;
    	this.msgId = 0;
    	this.rand = new Random();
         
    }
    
	
	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#nextTuple()
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void nextTuple() {
		long nbTuples = this.getNbTuples();
		JsonObjectBuilder r = null;
        JsonObject row  = null;

        logger.info("[BigDataSpout] emiting " + nbTuples + " tuples" );
        
		for(int i = 0 ; i < nbTuples; i++){
				r = Json.createObjectBuilder();
		        r.add("id", msgId);
		        r.add("nb", nbTuples);
				r.add("value", "" + this.initTimestamp);
		        row = r.build();
			    
		           
		    collector.emit(new Values(row.toString()), ++msgId);
		    Utils.sleep(500);
	    }


	}
	
	
	private long getNbTuples(){
		
		long inter = (System.currentTimeMillis() - this.initTimestamp) / 5000; 
		return  (long)Math.exp(inter);
	
		
	}
	
	
	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#open(java.util.Map, org.apache.storm.task.TopologyContext, org.apache.storm.spout.SpoutOutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
       
	}



	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		
	}

	

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object msgId) {
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void fail(Object msgId) {
	
	}


	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#declareOutputFields(org.apache.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		 declarer.declare(new Fields("json"));
		
	}

	/* (non-Javadoc)
	 * @see org.apache.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
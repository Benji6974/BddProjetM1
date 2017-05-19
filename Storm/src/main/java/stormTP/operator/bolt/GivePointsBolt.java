package stormTP.operator.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.StringReader;
import java.util.Map;

//import java.util.logging.Logger;


/**
 * Sample of stateless operator
 * @author lumineau
 *
 */
public class GivePointsBolt implements IRichBolt {

	private static final long serialVersionUID = 4262369370788107343L;
	private OutputCollector collector;

	public GivePointsBolt () {

	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#execute(backtype.storm.tuple.Tuple)
	 */
	public void execute(Tuple t) {

		String s = t.getString(0);
		JsonObject json = Json.createReader(new StringReader(s)).readObject();

		JsonObjectBuilder r = Json.createObjectBuilder();
		r.add("id", json.getInt("id"));
		r.add("name", json.getString("name"));
		r.add("team", json.getString("team"));
		int point = 0;
		int x = json.getInt("x");
		int y = json.getInt("y");

		if ((x >=6 && x <=9 &&
				(y >=3 && y <=6 || y >=15 && y <=18)) ||
			(x >=17 && x <=24 &&
					y >=7 && y <=14) ||
			(x >=32 && x <=35 &&
				(y >=3 && y <=6 || y >=15 && y <=18))){
			if (json.getString("color").equals("black")){
				point = 1;
			}else if (json.getString("color").equals("red")){
				point = 3;
			}
		}else{
			point = 0;
		}
		System.out.println("***point "+point +" "+x+" "+y+" "+json.getString("color") );

		r.add("points", point);
		JsonObject row = r.build();
		collector.emit(t,new Values(row.toString()));

		return;

	}



	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("json"));
	}


	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#cleanup()
	 */
	public void cleanup() {

	}



	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
}
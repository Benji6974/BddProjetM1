package stormTP.operator.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.Map;


public class InfoScoreBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
	private static final long serialVersionUID = 4262379330788107343L;
    private  KeyValueState<String, Integer> state;
    private  int sum;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        this.state = state;
        sum = state.get("sum", 0);
        System.out.println("initState with state [" + state + "] current sum [" + sum + "]");
    }

    @Override
    public void execute(TupleWindow inputWindow) {
    	
    	int cpt = 0;
        int point = 0;
        String team = "";
        String name = "";
        long firstId = 0;
		long lastId = 0;
        int lastScore = 0;
        String evol = "";

        for (Tuple t : inputWindow.get()) {
        	cpt++;

            //String s = t.getString(0);
            //JsonObject json = Json.createReader(new StringReader(s)).readObject();
            if (cpt == 1){
                //firstId = json.getInt("id");
                firstId = t.getLongByField("id");
            }else if (cpt == inputWindow.get().size()){
                //lastId = json.getInt("id");
                lastId = t.getLongByField("id");
            }

            /*name = json.getString("name");
            team = json.getString("team");
            sum += json.getInt("points");*/
            name = t.getString(1);
            team = t.getString(2);
            sum += t.getInteger(3);
            state.put("sum", sum);
            //lastScore += json.getInt("points");
            lastScore += t.getInteger(3);

    	}
        state.put("sum", cpt);


        if (lastScore > 0){
            evol = "croissant";
        }else if (lastScore < 0){
            evol = "decroissant";
        }else if (lastScore == 0){
            evol = "constant";
        }

        JsonObjectBuilder r = Json.createObjectBuilder();
        r.add("id", firstId+"-"+lastId);
        r.add("name", name);
		r.add("team", team);
        r.add("totalScore", sum);
        r.add("lastPoints", lastScore);
        r.add("evol", evol);
        JsonObject row = r.build();
	    
        collector.emit(inputWindow.get(),new Values(row.toString()));
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}


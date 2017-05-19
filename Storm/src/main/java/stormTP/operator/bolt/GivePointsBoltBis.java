package stormTP.operator.bolt;

import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.util.Map;


public class GivePointsBoltBis extends BaseStatefulBolt<KeyValueState<String, Integer>> {
	private static final long serialVersionUID = 4262379330722107343L;
    KeyValueState<String, Integer> kvState;
    int sum;
    private OutputCollector collector;

    
    @Override
    public void execute(Tuple t) {
        String s = t.getString(0);
        JsonObject json = Json.createReader(new StringReader(s)).readObject();
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
        long id = (long)json.getInt("id");
        collector.emit(t,new Values(id,json.getString("name"),json.getString("team"),point));
    }

    @Override
    public void initState(KeyValueState<String, Integer> state) {
        kvState = state;
        sum = kvState.get("sum", 0);
        
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","name","team","point"));
    }


}
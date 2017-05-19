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
import java.util.HashMap;
import java.util.Map;


public class PodiumBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
    private static final long serialVersionUID = 4262379338788107343L;
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


        HashMap<String,Integer> scores = new HashMap<>();
        for (Tuple t : inputWindow.get()) {
            if (scores.containsKey(t.getString(1))) {
                int score = scores.get(t.getString(1)) + t.getInteger(3);
                scores.put(t.getString(1), score);
            }else{
                scores.put(t.getString(1), t.getInteger(3));
            }

        }

        JsonObjectBuilder r = Json.createObjectBuilder();
        r.add("team","c");
        String top[] = {"","",""};
        for(int y = 0; y<3; y++){
            int max = -1;
            for(Map.Entry<String, Integer> entry : scores.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
                if (max < value){
                    max = value;
                    top[y] = key;
                }
            }
            scores.remove(top[y]);
        }

        r.add("rank1", top[0]);
        r.add("rank2", top[1]);
        r.add("rank3", top[2]);

        JsonObject row = r.build();

        collector.emit(inputWindow.get(),new Values(row.toString()));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}


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
import java.util.ArrayList;
import java.util.Map;



public class CheckPointsBolt extends BaseStatefulWindowedBolt<KeyValueState<String, Integer>> {
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


        ArrayList<Long> id = new ArrayList<>();
        ArrayList<String> name = new ArrayList<>();
        ArrayList<String> team = new ArrayList<>();
        ArrayList<Integer> point = new ArrayList<>();
        System.out.println("***sizetp"+inputWindow.get().size());
        if (inputWindow.get().size() >= 2) {

            System.out.println("***jesuis");
            for (Tuple t : inputWindow.get()) {
                id.add(t.getLongByField("id"));

                name.add(t.getStringByField("name"));
                team.add(t.getStringByField("team"));
                point.add(t.getInteger(3));
            }

            if (id.get(0) == id.get(1)) {
                JsonObjectBuilder r = Json.createObjectBuilder();
                r.add("name", name.get(0));
                r.add("idscore", id.get(0));
                if (point.get(0) == point.get(1)) {
                    r.add("consistent", "OK");
                } else {
                    r.add("consistent", "Houston, We've Had a Problem !");
                }
                JsonObject row = r.build();

                collector.emit(inputWindow.get(), new Values(row.toString()));
            }


    }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}


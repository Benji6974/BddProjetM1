package stormTP.operator.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.StringReader;
import java.util.Map;


public class AnomalyDetectionBolt extends BaseWindowedBolt {
	private static final long serialVersionUID = 4262387370788107343L;
	private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
    	
    	int cpt = 0;

        int xAnt = 0;
        int yAnt = 0;
        int x = 0;
        int y = 0;
        int id = 0;
        boolean anomaly = false;
        if (inputWindow.get().size() >= 2){
            System.out.println("***cpt "+ cpt);
            Tuple actu = inputWindow.get().get(1);
            System.out.println("***test ");
            String sactu = actu.getString(0);
            JsonObject jsonActu = Json.createReader(new StringReader(sactu)).readObject();
            id = jsonActu.getInt("id");

            if (cpt>1){
                Tuple ante = inputWindow.get().get(0);
                String sante = ante.getString(0);
                JsonObject jsonAnte = Json.createReader(new StringReader(sante)).readObject();
                xAnt = jsonAnte.getInt("x");
                yAnt = jsonAnte.getInt("y");

                x = jsonActu.getInt("x");
                y = jsonActu.getInt("y");


                if ((x == xAnt-1 || x == xAnt+1 || x == xAnt || (xAnt == 40 && x == 1) || (xAnt == 1 && x == 40)) &&
                        ( y == yAnt-1 || y == yAnt+1 || y == yAnt || (yAnt == 20 && y == 1) || (yAnt == 1 && y == 20))){
                    anomaly = false;
                }else{
                    anomaly = true;
                }
            }

            JsonObjectBuilder r = Json.createObjectBuilder();
            r.add("id", id);
            r.add("Anomaly", anomaly);
            JsonObject row = r.build();

            collector.emit(inputWindow.get(),new Values(row.toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }
}







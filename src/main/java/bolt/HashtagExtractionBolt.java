package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.StringTokenizer;

public class HashtagExtractionBolt extends BaseRichBolt {
    private OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getStringByField("message");
        StringTokenizer st = new StringTokenizer(text);

        //System.out.println("---- Split by space ------");
        while (st.hasMoreElements()) {

            String term = (String) st.nextElement();
            if (StringUtils.startsWith(term, "#")){
                String hashtag = term;
                //TODO emit hashtag
                _collector.emit(new Values(hashtag));
            }

        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("entity"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}

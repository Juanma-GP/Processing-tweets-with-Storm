package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.tuple.Values;
import org.apache.commons.lang.ObjectUtils;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.Status;

/**
 * Created with IntelliJ IDEA.
 * User:
 * Date: 05.04.2018
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class KloutScore extends BaseRichBolt {
    PrintWriter writer;
    private OutputCollector _collector;
    private static String kloutKey;
    public KloutScore(String kloutkey){
        this.kloutKey = kloutkey;
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            String results = tuple.getStringByField("Results");
            String cadenaUrl;

            cadenaUrl = "http://api.klout.com/v2/user.json/";
            cadenaUrl += results;
            cadenaUrl += "/score" + "?key=" + kloutKey;

            URL url = new URL(cadenaUrl);
            HttpURLConnection c = (HttpURLConnection) url.openConnection();
            c.setRequestMethod("GET");
            c.setRequestProperty("Content-length", "0");
            c.setUseCaches(false);
            c.setAllowUserInteraction(false);
            c.connect();
            int status = c.getResponseCode();
            StringBuilder sb = new StringBuilder();
            switch (status) {
                case 200:
                case 201:
                    BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    String line;
                    while ((line = br.readLine()) != null) sb.append(line + "\n");
                    br.close();
            }

            JSONParser parser= new JSONParser();
            org.json.simple.JSONObject jsonResponse = (org.json.simple.JSONObject) parser.parse(sb.toString());
            Double score = (Double) jsonResponse.get("score");
            String score2 = Double.toString(score);
            _collector.emit(new Values(tuple.getStringByField("ScreenName"),results,score2));

            _collector.ack(tuple);
        }
        // JSONException |
        catch(  ParseException | IOException e ){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // "Text","Results",
        outputFieldsDeclarer.declare(new Fields("ScreenName","Results","Score"));
    }

    @Override
    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}

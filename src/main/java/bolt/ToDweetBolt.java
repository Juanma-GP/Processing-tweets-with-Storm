package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import twitter4j.JSONArray;
import twitter4j.JSONObject;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.StringTokenizer;

public class ToDweetBolt extends BaseRichBolt {
    private OutputCollector _collector;

    private String lat = "0.0";
    private String lng = "0.0";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String url = "";
        try {
            String texto = new String(tuple.getStringByField("texto"));
            String lugar = new String(tuple.getStringByField("lugar"));
            String latitud = new String(tuple.getStringByField("latitud"));
            String longitud = new String(tuple.getStringByField("longitud"));
            String dataSource = "camera_blanco";

            url = "https://dweet.io/dweet/for/";
            url += dataSource;
            url += "?long=" + longitud;
            url += "&lat=" + latitud;
            url += "&text=" + texto.replace(' ', '+');
            url += "&category=Place&entity=" + lugar;

            URL peticion = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) peticion.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-length", "0");
            connection.setUseCaches(false);
            connection.setAllowUserInteraction(false);
            connection.connect();
            int respuesta=connection.getResponseCode();
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = br.readLine()) != null)
                response.append(inputLine);

            _collector.emit(new Values());

            // Confirmación de que la tupla ya ha sido tratada
            _collector.ack(tuple);

            /*twitter4j.JSONException | IOException.Malformed |*/
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields());
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
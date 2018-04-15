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

public class GeolocationBolt extends BaseRichBolt {
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

            // tuple es la tupla emitida en el bolt entities

            // Se convierte el valor que contiene la tupla (que es de tipo String) en un objeto json

            String texto = new String(tuple.getStringByField("message"));
            String lugar = new String(tuple.getStringByField("lugar"));

            lugar = lugar.replace(' ', '+');
            System.out.println(lugar);
                //Se puede eliminar el if, siempre es true
                /*lugar = lugar.replaceAll("[^\\p{Alpha}\\p{Digit}]+","+").replace(' ', '+')*/
            url = "http://maps.google.com/maps/api/geocode/json?address="+lugar+"&sensor=false";
            URL peticion = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) peticion.openConnection();
            connection.connect();

            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = br.readLine()) != null)
                response.append(inputLine);
            System.out.println("\n"+response+"\n");

            JSONObject jsongoogle = new JSONObject(response.toString());
            JSONArray jarraygoogle = jsongoogle.getJSONArray("results");
            JSONObject jobjectgoogle = jarraygoogle.getJSONObject(0);
            JSONObject geo = jobjectgoogle.getJSONObject("geometry").getJSONObject("location");
            this.lat = geo.getString("lat");
            this.lng = geo.getString("lng");



            //JSONObject sal = new JSONObject();sal.append("lat", this.lat);sal.append("lng", this.lng);sal.append("lugar", lugar);salida.put(sal);json.put("location", salida);

            _collector.emit(new Values(texto,lugar,this.lat,this.lng));

            // Confirmación de que la tupla ya ha sido tratada
            _collector.ack(tuple);

        } catch (twitter4j.JSONException | IOException e){
            System.out.println("URL: " + url);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("texto","lugar","latitud","longitud"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
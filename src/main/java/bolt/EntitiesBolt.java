package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;


import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class EntitiesBolt extends BaseRichBolt {

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }
    private String dandelion_key;
    public EntitiesBolt(String clave){
        this.dandelion_key = clave;
    }
    @Override
    public void execute(Tuple tuple) {
        String dandelion_key_1 = "sodolipiaescotilisgifralicapersu";
        String dandelion_key_buena=dandelion_key;

        try {
            String text = tuple.getStringByField("message");
            String cadenaUrl;

            cadenaUrl = "https://api.dandelion.eu/datatxt/nex/v1/?social=True&min_confidence=0.6";
            cadenaUrl += "&country=-1&include=image%2Cabstract%2Ctypes%2Ccategories%2Clod&text=";
            cadenaUrl += text.replaceAll("[^\\p{Alpha}\\p{Digit}]+","+").replace(' ', '+');
            cadenaUrl += "&token=" + dandelion_key_buena;
            System.out.println("\n\n"+cadenaUrl+"\n\n");
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
                    while ((line = br.readLine()) != null) {
                        sb.append(line+"\n");
                    }
                    br.close();
                case 401:
                    System.out.println("\n\nSe nos han acabado las unidades de Dandelion\n\n"   );
            }

            JSONObject jsonResponse = new JSONObject(sb.toString());
            JSONArray results = jsonResponse.getJSONArray("annotations");
            for (int i=0; i<results.length();i++) {
                String categoria = "Otro";
                JSONObject annotation = results.getJSONObject(i);
                String label = annotation.getString("label");
                if (label.equals("HTTPS") || label.equals("HTTP")) continue;
                String nombre = annotation.getString("title"); //------------------------------//
                JSONArray tipos = annotation.getJSONArray("types");
                for (int j= 0; j< tipos.length(); j++) {
                    String tipo = tipos.getString(j).substring(28);
                    if (tipo.equals("Place") || tipo.equals("Location")) {
                        categoria = "Place";
                    }
                    else {
                        categoria = "Otro";
                    }
                }
                if (categoria.equals("Place")) {
                    _collector.emit(new Values(text,nombre));
                    // Confirmación de que la tupla fue creada
                    _collector.ack(tuple);
                }

            }

        }
        catch( JSONException | IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message","lugar"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
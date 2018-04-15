package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import util.Persona;

/**
 * Created with IntelliJ IDEA.
 * User:
 * Date: 05.04.2018
 * Time: 15:56
 * To change this template use File | Settings | File Templates.
 */
public class FileWriterBolt extends BaseRichBolt {
    PrintWriter writer;
    private OutputCollector _collector;
    private String filename;
    public ArrayList<Persona> influencers =new ArrayList<Persona>();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");


    public FileWriterBolt(String filename){
        this.filename = filename;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        try {
            writer = new PrintWriter(filename, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Override
    public void execute(Tuple tuple) {

        Persona nueva = new Persona(tuple.getStringByField("ScreenName"), Long.parseLong(tuple.getStringByField("Results")),
                                                                             Double.parseDouble(tuple.getStringByField("Score")));

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        if(influencers.size()<5){
            int i = influencers.size()-1;
            String elQueLlega = "\tName:  "+nueva.getScreenName()+ ".\n\t   Id: "+
                    String.valueOf(nueva.getIdKlout())     +   ".\n\tScore: " +  String.valueOf(nueva.getScoreKlout())+ ".\n";

            if(i<0){ influencers.add(nueva); System.out.println(timestamp+"\n"+String.valueOf(i)+"\n"+elQueLlega+"TODO VA BIEN--1"); writer.println(timestamp+"\n"+String.valueOf(i)+"\n"+elQueLlega+"TODO VA BIEN--1");
            }
            else if(i>=0 && influencers.get(i).getScoreKlout()>=nueva.getScoreKlout()){ influencers.add(nueva); System.out.println(timestamp+"\n"+String.valueOf(i)+"\n"+elQueLlega+"TODO VA BIEN--2"); writer.println(timestamp+"\n"+String.valueOf(i)+"\n"+elQueLlega+"TODO VA BIEN--2");
            }
            else {
                while ((i >= 0) && (influencers.get(i).getScoreKlout() < nueva.getScoreKlout())) {
                    Persona auxiliar = new Persona(influencers.get(i).getScreenName(), influencers.get(i).getIdKlout(),
                            influencers.get(i).getScoreKlout());
                    influencers.set(i, nueva);
                    if ((i < 4) && (influencers.size() < 5)) {
                        if (i>=influencers.size()-1) influencers.add(auxiliar);
                        else influencers.set(i + 1, auxiliar);
                    }
                    i--;
                }
                for(int j=0;j<influencers.size()-1;j++){
                    System.out.println(timestamp+"\n"+j+"\tName:  "+influencers.get(j).getScreenName()+
                            ".\n\t   Id: "+String.valueOf(influencers.get(j).getIdKlout())+
                            ".\n\tScore: "+String.valueOf(influencers.get(j).getScoreKlout())+
                            ".\n"+"TODO VA BIEN-ordenamos");
                    writer.println(timestamp+"\n"+j+"\tName:  "+influencers.get(j).getScreenName()+
                            ".\n\t   Id: "+String.valueOf(influencers.get(j).getIdKlout())+
                            ".\n\tScore: "+String.valueOf(influencers.get(j).getScoreKlout())+
                            ".\n"+"TODO VA BIEN-ordenamos");
                }
            }
            /*System.out.println(timestamp+":::"+tuple.getStringByField("ScreenName")+" -- Id:"+tuple.getStringByField("Results")+ " -- Score:"+tuple.getStringByField("Score"));*/
        }
        else if (influencers.size()==5){ // influencers.size()==paraComprobar.size()==5
            int i = 4;
            if(influencers.get(i).getScoreKlout()<nueva.getScoreKlout()) {
                while ((i >= 0) && (influencers.get(i).getScoreKlout() < nueva.getScoreKlout())) {
                    if (i < 4) {
                        Persona auxiliar = new Persona(influencers.get(i).getScreenName(), influencers.get(i).getIdKlout(),
                                influencers.get(i).getScoreKlout());
                        influencers.set(i + 1, auxiliar);
                    }
                    influencers.set(i, nueva);
                    i--;
                }
                for(int j=0;j<5;j++){
                    //System.out.println(timestamp+"\n"+j+"\tName:  "+influencers.get(j).getScreenName()+".\n\t   Id: "+String.valueOf(influencers.get(j).getIdKlout())+".\n\tScore: "+      String.valueOf(influencers.get(j).getScoreKlout())+ ".\n");
                    writer.println(timestamp+"\n"+j+"\tName:  " + influencers.get(j).getScreenName() + ".\n\t   Id: " +
                            String.valueOf(influencers.get(j).getIdKlout()) + ".\n\tScore: " +
                            String.valueOf(influencers.get(j).getScoreKlout()) + ".\n");
                }
            }
        }
        writer.flush();
        // Confirm that this tuple has been treated.
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}

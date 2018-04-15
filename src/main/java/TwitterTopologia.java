import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import bolt.*;
import spout.TwitterSpout;
import twitter4j.FilterQuery;


public class TwitterTopologia {
    private static String consumerKey; // = "sodolipiaescotilisgifralicapersu";
    private static String consumerSecret; // = "sodolipiaescotilisgifralicapersu";
    private static String accessToken; // = "sodolipieascotilisgifralicapersu";
    private static String accessTokenSecret; // = "sodolipiaescotilisgifralicapersu";
    private static String dandelionKey; //="sodolipiaescotilisgifralicapersu";
    private static String kloutKey; //="sodolipiaescotilisgifralicapersu";
    private static String remoteClusterTopologyName;

    public static void main(String[] args) throws Exception {
        /**************** SETUP ****************/
        String remoteClusterTopologyName = null;
        if (args!=null) {
            if (args.length==1) {
                remoteClusterTopologyName = args[0];
            } else if (args.length==3) {
                remoteClusterTopologyName = args[0];
                kloutKey=args[1];
                dandelionKey=args[2];
            }
            // If credentials are provided as commandline arguments
            else if (args.length==7) {
                remoteClusterTopologyName = args[0];
                kloutKey=args[1];
                dandelionKey=args[2];
                consumerKey =args[3];
                consumerSecret =args[4];
                accessToken =args[5];
                accessTokenSecret =args[6];
            }
        }
        /****************       ****************/

        TopologyBuilder builder = new TopologyBuilder();
        FilterQuery tweetFilterQuery = new FilterQuery();

        // TODO: Define your own twitter query
        tweetFilterQuery.track(new String[]{"Trump","USA"});// See https://github.com/kantega/storm-twitter-workshop/wiki/Basic-Twitter-stream-reading-using-Twitter4j
        tweetFilterQuery.language(new String[]{"en","es"});

        //tweetFilterQuery.locations(new double[][]{new double[]{-126.562500,30.448674}, new double[]{-61.171875,44.087585}});
        //
        TwitterSpout spout = new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, tweetFilterQuery);
        //TODO: Set the twitter spout as spout on this topology. Hint: Use the builder object.

        KloutBuscador buscamosEnKlout = new KloutBuscador(kloutKey);
        KloutScore scoreEnKlout = new KloutScore(kloutKey);
        FileWriterBolt fileWriterBolt = new FileWriterBolt("/tmp/storm-pruebas/influencers.txt");

        //TODO: Route messages from the spout to the file writer bolt. Hint: Again, use the builder object.
        //LanguageDetectionBolt lenguaje= new LanguageDetectionBolt();
        //HashtagExtractionBolt hashtag = new HashtagExtractionBolt();
        //RollingCountBolt contador = new RollingCountBolt(9, 3);
        //EntitiesBolt entitiesBolt = new EntitiesBolt(dandelionKey);
        //GeolocationBolt geolocalizacion = new GeolocationBolt();
        //ToDweetBolt vamosAmostrarlo = new ToDweetBolt();


        builder.setSpout("spoutLeerTwitter",spout,1);
        builder.setBolt("buscamosEnKlout",buscamosEnKlout,1).shuffleGrouping("spoutLeerTwitter");
        builder.setBolt("scoreEnKlout",scoreEnKlout,1).shuffleGrouping("buscamosEnKlout");
        builder.setBolt("escribirFichero",fileWriterBolt,1).shuffleGrouping("scoreEnKlout");
        //builder.setBolt("lenguaje",lenguaje,1).shuffleGrouping("spoutLeerTwitter");
        //builder.setBolt("hashtag",hashtag,1).shuffleGrouping("lenguaje");
        //builder.setBolt("cont",contador,1).shuffleGrouping("hashtag");
        //builder.setBolt("buscamosEnKlout",buscamosEnKlout,1).shuffleGrouping("spoutLeerTwitter");
        //builder.setBolt("escribirFichero",fileWriterBolt,1).shuffleGrouping("buscamosEnKlout");
        //builder.setBolt("extraerEntidades",entitiesBolt,1).shuffleGrouping("spoutLeerTwitter");
        //builder.setBolt("geoLocalizacion",geolocalizacion,1).shuffleGrouping("extraerEntidades");
        //builder.setBolt("loMostramos", vamosAmostrarlo, 1).shuffleGrouping("geoLocalizacion");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(remoteClusterTopologyName, conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("twitter-fun", conf, builder.createTopology());

            Thread.sleep(460000);

            cluster.shutdown();
        }
    }
}

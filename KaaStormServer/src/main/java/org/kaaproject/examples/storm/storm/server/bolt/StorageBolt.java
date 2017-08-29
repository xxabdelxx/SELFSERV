package org.kaaproject.examples.storm.storm.server.bolt;

import java.util.Map;
import java.util.Properties;

import org.bson.Document;
import org.kaaproject.examples.storm.storm.server.Main;
import org.kaaproject.examples.storm.storm.server.producer.AvroFlumeEventProducer;
import project.selfserv.kaa.sensors.data.sensorsDataCollection;

import org.kaaproject.kaa.server.common.log.shared.KaaFlumeEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import project.selfserv.kaa.sensors.data.acceletometer.acceleroMetersensor;

public class StorageBolt implements IRichBolt {

	    private static final Logger LOG = LoggerFactory.getLogger(StorageBolt.class);
	    public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-avro-forward";

	    private static final KaaFlumeEventReader<sensorsDataCollection> kaaReader = new KaaFlumeEventReader<sensorsDataCollection>(sensorsDataCollection.class);
	    private AvroFlumeEventProducer producer;
	    private OutputCollector collector;
	    private Main fromMainClass;
	    // To connect to mongodb server
       // static MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			
        // Now connect to your databases
       // static com.mongodb.DB db = mongoClient.getDB( "test" );
       // static DBCollection coll = db.getCollection("mycol");
        
	    public StorageBolt()
	    {
	    	fromMainClass = new Main();
	    }

	    public String getFlumePropertyPrefix() {
	        return DEFAULT_FLUME_PROPERTY_PREFIX;
	    }

	    public void setProducer(AvroFlumeEventProducer producer) {
	        this.producer = producer;
	    }

	    @SuppressWarnings("rawtypes")
	    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
	        this.collector = collector;
	        Properties sinkProperties  = new Properties();
	        LOG.info("Looking for flume properties");
	        for (Object key : config.keySet()) {
	            if (key.toString().startsWith(this.getFlumePropertyPrefix())) {
	                LOG.info("Found:Key:" + key.toString() + ":" + config.get(key));
	                sinkProperties.put(key.toString().replace(this.getFlumePropertyPrefix() + ".",""),
	                        config.get(key));
	            }
	        }
	    }
	    
	    public void execute(Tuple input) {
	        try {

	        	  System.out.println("id--->"+input.getSourceComponent());
				  //samples SensorsData= (samples) ((sensorsDataCollection) input.getValueByField("SensorsData")).getSamples();
				  
	        	  LOG.info("#################### Sending Event ##############");
	        	  fromMainClass.getEventsManager().sendEvent((long)123456789,69);
	        	  
	        	  
				  float CGM_Value = input.getFloatByField("CGM_Value");
				  float HeartRatesensor_Value = input.getFloatByField("HeartRatesensor_Value");
				  float bodyTemperaturesensor_Value = input.getFloatByField("bodyTemperaturesensor_Value");
				  acceleroMetersensor acceleroMetersensor_Value = (acceleroMetersensor) input.getValueByField("acceleroMetersensor_Value");
				  float xAxis = acceleroMetersensor_Value.getXAxis();
				  float yAxis = acceleroMetersensor_Value.getYAxis();
				  float zAxis = acceleroMetersensor_Value.getZAxis();
				  float galvanicSkinRespsensor_Value = input.getFloatByField("galvanicSkinRespsensor_Value");
	           
	              LOG.info("SensorsData in Storage bolt: ----------------------------------------------------");
	              LOG.info("CGM_Value : "+CGM_Value);
	              LOG.info("HeartRatesensor_Value : "+HeartRatesensor_Value);
	              LOG.info("bodyTemperaturesensor_Value : "+bodyTemperaturesensor_Value);
	              LOG.info("acceleroMetersensor (x,y,z) : ("+xAxis+","+yAxis+","+zAxis+")");
	              LOG.info("galvanicSkinRespsensor_Value : "+galvanicSkinRespsensor_Value);
	          
	              Document document = new Document("title", "MongoDB") 
        	      .append("id", 1)
        	      .append("description", "database") 
        	      .append("likes", 100) 
        	      .append("url", "http://www.tutorialspoint.com/mongodb/") 
        	      .append("by", "tutorials point");  

	              fromMainClass.getDbInstance().insertDocument("Monitoring", document);

	            //All seems to be nice, notify storm.spout about it
	            this.collector.ack(input);
	        } catch (Exception e) {
	            LOG.warn("Failing tuple: " + input);
	            LOG.warn("Exception: ", e);
	            //Notify storm.spout about fail
	            this.collector.fail(input);
	        }
	    }

		@Override
	    public void cleanup() {
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    	declarer.declare(new Fields(   
                    "timestamp",
                    "CGM_Value",
                    "HeartRatesensor_Value",
                    "bodyTemperaturesensor_Value",
                    "acceleroMetersensor_Value",
                    "galvanicSkinRespsensor_Value"
                )
    );
	    }

	    @Override
	    public Map<String, Object> getComponentConfiguration() {
	        return null;
	    }
	}

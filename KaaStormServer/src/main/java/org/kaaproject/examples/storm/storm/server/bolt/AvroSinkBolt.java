package org.kaaproject.examples.storm.storm.server.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.flume.Event;
import org.kaaproject.examples.storm.storm.server.Main;
import org.kaaproject.examples.storm.storm.server.producer.AvroFlumeEventProducer;

import project.selfserv.events.EventsManager;
import project.selfserv.kaa.sensors.data.sensorsDataCollection;
import project.selfserv.kaa.sensors.data.acceletometer.acceleroMetersensor;
import project.selfserv.kaa.sensors.data.samples.samples;
import org.kaaproject.kaa.client.event.FindEventListenersCallback;

import org.kaaproject.kaa.server.common.log.shared.KaaFlumeEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("serial")
public class AvroSinkBolt implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSinkBolt.class);
    public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-avro-forward";
    Main EventInstance;

    private static final KaaFlumeEventReader<sensorsDataCollection> kaaReader = new KaaFlumeEventReader<sensorsDataCollection>(sensorsDataCollection.class);
    private AvroFlumeEventProducer producer;
    private OutputCollector collector;
    
    public AvroSinkBolt()
    {
    	
        
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
        	
            Event event = this.producer.toEvent(input);
            for(sensorsDataCollection report : kaaReader.decodeRecords(ByteBuffer.wrap(event.getBody())))
            {
            
              LOG.info("--------------------MESSAGE---------------------------- ");
              LOG.info("Time : "+report.getTimestamp().toString());

              //LOG.info("SAmples : "+report.getSamples());
              
			  List<samples> SensorsData= new ArrayList<samples>();
			  SensorsData =  report.getSamples();
			  float CGM_Value = SensorsData.get(0).getCGMsensor();
			  float HeartRatesensor_Value = SensorsData.get(0).getHeartRatesensor();
			  float bodyTemperaturesensor_Value = SensorsData.get(0).getBodyTemperaturesensor();
			  acceleroMetersensor acceleroMetersensor_Value = (acceleroMetersensor) SensorsData.get(0).getAcceleroMetersensor().get(0);
			  float xAxis = acceleroMetersensor_Value.getXAxis();
			  float yAxis = acceleroMetersensor_Value.getYAxis();
			  float zAxis = acceleroMetersensor_Value.getZAxis();
			  float galvanicSkinRespsensor_Value = SensorsData.get(0).getGalvanicSkinRespsensor();
			  
              LOG.info("------------------Sensors Data--------------------------------");
              LOG.info("CGM_Value : "+CGM_Value);
              LOG.info("HeartRatesensor_Value : "+HeartRatesensor_Value);
              LOG.info("bodyTemperaturesensor_Value : "+bodyTemperaturesensor_Value);
              LOG.info("acceleroMetersensor (x,y,z) : ("+xAxis+","+yAxis+","+zAxis+")");
              LOG.info("galvanicSkinRespsensor_Value : "+galvanicSkinRespsensor_Value);
              
              LOG.info("========================================================>Sending to recordBolt");
			  //send to recordBolt
              this.collector.emit(new Values(
	            		                         report.getTimestamp(),
	            		                         CGM_Value,
	            		                         HeartRatesensor_Value,
	            		                         bodyTemperaturesensor_Value,
	            		                         acceleroMetersensor_Value,
	            		                         galvanicSkinRespsensor_Value
            		              			)
            		  			  );
            }
            LOG.info("========================================================>sent to recordBolt");
            //All seems to be nice, notify storm.spout about it
            this.collector.ack(input);
            LOG.info("========================================================>notify storm.spout done");
        } catch (Exception e) {
            LOG.warn("Failing tuple: " + input);
            LOG.warn("Exception: ", e.getMessage());
            //Notify storm.spout about fail
            this.collector.fail(input);
        }
    }

   
    public static long getCurrentTime(){
		//get the current timestamp
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return ((timestamp.getTime()/1000)+(timestamp.getTime()%1));
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

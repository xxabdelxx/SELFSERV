/*
 * Copyright 2014-2016 CyberVision, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kaaproject.examples.storm.storm.server;


import java.io.Serializable;
import java.util.Properties;

import org.kaaproject.examples.storm.storm.server.bolt.AbstractMongoBolt;
import org.kaaproject.examples.storm.storm.server.bolt.AvroSinkBolt;
import org.kaaproject.examples.storm.storm.server.bolt.MongoInsertBolt;
import org.kaaproject.examples.storm.storm.server.bolt.StorageBolt;
import org.kaaproject.examples.storm.storm.server.producer.AvroTupleProducer;
import org.kaaproject.examples.storm.storm.server.producer.SimpleAvroFlumeEventProducer;
import org.kaaproject.examples.storm.storm.server.producer.SimpleAvroTupleProducer;
import org.kaaproject.examples.storm.storm.server.spout.FlumeSourceSpout;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import project.selfserv.events.EventsManager;
import project.selfserv.storage.mongodb.*;


public class Main implements Serializable{
	
	private static EventsManager EM;
	private static MongoDbManager MDBM;
	static String url = "mongodb://selfservteam:selfserv123@192.168.100.210/selfservdb";
	static String collectionName = "Monitoring";
	
	public EventsManager getEventsManager()
	{
		return EM;
	}
	
	public MongoDbManager getDbInstance()
	{
		return MDBM;		
	}
	
    public static void main(String[] args) throws Throwable{
        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);
        Logger LOG = (Logger)LoggerFactory.getLogger(Main.class);
        
        //connect to the storage unit
        MDBM = new MongoDbManager();
        if(!MDBM.startMongoDb())
        {
        	LOG.error("############### STORAGE UNIT : ERROR ############");
        	return;
        }
        LOG.info("############### STORAGE UNIT : OK ############");
        LOG.info("############### STARTING EVENTS ############");
        EM = new EventsManager();
    	if(!EM.start())
    	{
	    	LOG.error("############### EVENTS : ERROR       ############");
	    	return;
    	}
		LOG.info("############### EVENTS : OK            ############");
    /*    Document document = new Document("title", "MongoDB") 
        	      .append("id", 1)
        	      .append("description", "database") 
        	      .append("likes", 100) 
        	      .append("url", "http://www.tutorialspoint.com/mongodb/") 
        	      .append("by", "tutorials point");  

        MDBM.insertDocument("Monitoring", document);
    */        
        Properties props = new Properties();
        props.load(Main.class.getResourceAsStream("/storm.properties"));

        TopologyBuilder builder = new TopologyBuilder();
        FlumeSourceSpout spout = new FlumeSourceSpout();

        AvroTupleProducer producer = new SimpleAvroTupleProducer();
        spout.setAvroTupleProducer(producer);

        builder.setSpout("FlumeSourceSpout", spout).addConfigurations(props);

        AvroSinkBolt bolt = new AvroSinkBolt();
        bolt.setProducer(new SimpleAvroFlumeEventProducer());
        //Set 2 threads storm.bolt
        builder.setBolt("AvroSinkBolt", bolt, 2).shuffleGrouping("FlumeSourceSpout").addConfigurations(props);
        
        
       /* StorageBolt db = new StorageBolt();
        builder.setBolt("StorageBolt",db,2).shuffleGrouping("AvroSinkBolt");
        */
       String url = "mongodb://192.168.100.210:27017/selfservdb";

        MongoMapper mapper = new SimpleMongoMapper()
        .withFields("word", "count");

        MongoInsertBolt insertBolt = new MongoInsertBolt(url, collectionName,mapper);
        builder.setBolt("MongoInsertBolt",insertBolt,2).shuffleGrouping("AvroSinkBolt");
        
       // Algorithm algorithm=new Algorithm();
       // builder.setBolt("Algorithm", algorithm,2).shuffleGrouping("AvroSinkBolt");

        Config config = new Config(); //Default configuration
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("T1", config, builder.createTopology());
        LOG.info("Topology running...");
        
        //Wait for any input and kill topology
       // System.in.read();
       // cluster.shutdown();
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kaaproject.examples.storm.storm.server.bolt;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.kaaproject.examples.storm.storm.server.common.BatchHelper;

import com.mongodb.MongoClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.ITuple;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleUtils;
import project.selfserv.kaa.sensors.data.acceletometer.acceleroMetersensor;
import project.selfserv.storage.mongodb.AbstractMongoBolt;
import project.selfserv.storage.mongodb.MongoMapper;

/**
 * Basic bolt for writing to MongoDB.
 * Note: Each MongoInsertBolt defined in a topology is tied to a specific collection.
 */
public class MongoInsertBolt extends AbstractMongoBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;
    private static final Logger LOG = LogManager.getLogger(MongoInsertBolt.class);

    private MongoMapper mapper;

    private boolean ordered = true;  //default is ordered.

    private int batchSize;

    private BatchHelper batchHelper;

    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    
    private String[] fields = { "timestamp",
					            "CGM_Value",
					            "HeartRatesensor_Value",
					            "bodyTemperaturesensor_Value",
					            "acceleroMetersensor_Value",
					            "galvanicSkinRespsensor_Value"
					            
    							};

    /**
     * MongoInsertBolt Constructor.
     * @param url The MongoDB server url
     * @param collectionName The collection where reading/writing data
     * @param mapper MongoMapper converting tuple to an MongoDB document
     */
    public MongoInsertBolt(String url, String collectionName, MongoMapper mapper) {
        super(url, collectionName);

        Validate.notNull(mapper, "MongoMapper can not be null");

        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (batchHelper.shouldHandle(tuple)) {
                batchHelper.addBatch(tuple);
                mongoClient.insert(toDocument(tuple, fields));
                  
            }

            if (batchHelper.shouldFlush()) {
                flushTuples();
                batchHelper.ack();
            }
        } catch (Exception e) {
            batchHelper.fail(e);
        }
    }

    private void flushTuples() {
        List<Document> docs = new LinkedList<>();
        for (Tuple t : batchHelper.getBatchTuples()) {
            Document doc = mapper.toDocument(t);
            docs.add(doc);
        }
        mongoClient.insert(docs, ordered);
    }

    public MongoInsertBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public MongoInsertBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public MongoInsertBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    @Override
    public void prepare(Map topoConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(topoConf, context, collector);
        this.batchHelper = new BatchHelper(batchSize, collector);
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
    
    public Document toDocument(ITuple input , String [] fields) {
        /*for(String field : fields){
            document.append(field, tuple.getValueByField(field));
        }
        //$set operator: Sets the value of a field in a document.
         */
        
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
    
        Document document = new Document("timestamp", input.getLongByField("timestamp")) 
	      .append("CGM_Value", input.getFloatByField("CGM_Value"))
	      .append("HeartRatesensor_Value" , input.getFloatByField("HeartRatesensor_Value")) 
	      .append("bodyTemperaturesensor_Value" , input.getFloatByField("bodyTemperaturesensor_Value")) 
	      .append("acceleroMetersensor_Value", 
	    		  new Document("xAxis" , acceleroMetersensor_Value.getXAxis())
	    		  .append("yAxis",acceleroMetersensor_Value.getYAxis())
	    		  .append("zAxis",acceleroMetersensor_Value.getZAxis())
	    		  )
	      .append("galvanicSkinRespsensor_Value", input.getFloatByField("galvanicSkinRespsensor_Value"));  
        return new Document(document);
    }
    
    public boolean isConnected()
    {
    	return mongoClient.isConnected();
    }

}

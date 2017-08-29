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

import java.util.Map;

import org.apache.commons.lang.Validate;
import org.kaaproject.examples.storm.storm.server.common.MongoDbClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;

public abstract class AbstractMongoBolt extends BaseRichBolt {

    private String url;
    private String collectionName;

    protected OutputCollector collector;
    protected MongoDbClient mongoClient;

    /**
     * AbstractMongoBolt Constructor.
     * @param url The MongoDB server url
     * @param collectionName The collection where reading/writing data
     */
    public AbstractMongoBolt(String url, String collectionName) {
        Validate.notEmpty(url, "url can not be blank or null");
        Validate.notEmpty(collectionName, "collectionName can not be blank or null");

        this.url = url;
        this.collectionName = collectionName;
    }

   @Override 
   public void prepare(Map topoConf, TopologyContext context,OutputCollector collector) {
        this.collector = collector;
        this.mongoClient = new MongoDbClient(url, collectionName);
    }

   @Override
    public void cleanup() {
        this.mongoClient.close();
    }
    
}
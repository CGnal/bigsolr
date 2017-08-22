/*
 * Licensed to Taka Shinagawa under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.bigsolr.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.HashMap;
import java.util.Map;

public class SolrOperations {

    private static Logger log = Logger.getLogger(SolrOperations.class);

    public static final String SERVER_URL = "solr.server.url";
    private static final String SERVER_MODE = "solr.server.mode";
    private static final String COLLECTION_NAME = "solr.server.collection";

    private static Map<String, SolrClient> solrClients = new HashMap<String, SolrClient>();
    private static final Map<String, CloudSolrClient> cachedClients = new HashMap<String, CloudSolrClient>();

    public static SolrClient getSolrClient(Configuration conf) {
        SolrClient solr = null;
        if (conf.get(SERVER_MODE).toLowerCase().equals("standalone")) {
            String ENDPOINT = "http://" + conf.get(SERVER_URL) + "/solr/" + conf.get(COLLECTION_NAME);
            solr = getSolrHttpClient(ENDPOINT);
        } else if (conf.get(SERVER_MODE).toLowerCase().equals("cloud")) {
            solr = getSolrCloudClient(conf.get(SERVER_URL), conf.get(COLLECTION_NAME));
        } else {
            log.error("This SERVER_MODE is not supported: " + conf.get(SERVER_MODE));
            System.exit(0);
        }
        return solr;
    }

    protected static HttpSolrClient getSolrHttpClient(String httpServerUrl) {
        // Better add authentication
        return new HttpSolrClient(httpServerUrl);
    }

    protected static CloudSolrClient getSolrCloudClient(String zkHostUrl, String collection) {
        CloudSolrClient cloudSolrClient = null;
        synchronized (cachedClients) {
            cloudSolrClient = cachedClients.get(zkHostUrl);
            if (cloudSolrClient == null) {
                cloudSolrClient = new CloudSolrClient(zkHostUrl + "/solr");
                cloudSolrClient.setDefaultCollection(collection);
                cloudSolrClient.connect();
                cachedClients.put(zkHostUrl, cloudSolrClient);
            }
        }
        return cloudSolrClient;
    }

}

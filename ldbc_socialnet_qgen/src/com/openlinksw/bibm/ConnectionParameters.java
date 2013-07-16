/*
 *  Big Database Semantic Metric Tools
 *
 * Copyright (C) 2011-2013 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package com.openlinksw.bibm;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Options;

public abstract class ConnectionParameters extends Options {
    public static final String version="Openlink BIBM Test Driver 0.7.7";
    public long startTime=System.currentTimeMillis();
    public Date date=new Date();

    BooleanOption versionOpt=new BooleanOption("version", 
            "prints the version of the Test Driver"); 
    BooleanOption helpOpt=new BooleanOption("help"
            ,"prints this help message"); 
    
    protected BooleanOption dryRun=new BooleanOption("dry-run"
            ,"makes parameter substition in queries but does not actually runs them."
            , "Implies -printres option");
  
    FileOption errFileName=new FileOption("err-log", null, 
            "log file name to write error messages", 
            "default: print errors only to stderr");
    
   public IntegerOption nrThreads=new IntegerOption("mt <Number of clients>", 1
           ,"Run multiple clients concurrently."
           ,"default: 1");
   
   public IntegerOption timeout=new IntegerOption("t <timeout in ms>", 0
           ,"Timeouts will be logged for the result report."
           ,"default: "+0);
   
    //SQL
    public BooleanOption doSQL=new BooleanOption("sql"
            ,"use JDBC connection to a RDBMS. Instead of a SPARQL-Endpoint, a JDBC URL has to be supplied."
            ,"default: not set");
    
    public StringOption driverClassName=new StringOption("dbdriver <DB-Driver Class Name>", "com.mysql.jdbc.Driver"
            ,"default: %%");

    // SPARQL
    protected StringOption baseEndpoint=new StringOption("url <common prefix for all endpoints>", ""
            ,"url <common prefix for all endpoints>"
            ,"default: empty string");
    protected MultiStringOption updateEndpoints=new MultiStringOption("u <Sparql Update Service Endpoint URL>"
            ,"Use this if you have SPARQL Update queries in your query mix.");
    
    public StringOption sparqlUpdateQueryParameter=new StringOption("uqp <update query parameter>",  "update"
            ,"The forms parameter name for the query string."
            ,"default: %%");

    public StringOption defaultGraph=new StringOption("dg <default graph>", null
            ,"add &default-graph-uri=<default graph> to the http request");
    
    public StringOption retryErrorMessage=new StringOption("retry-msg <message from server indicating deadlock>", null // for Virtuoso, "40001";
            ,"default: <null, that does not match any string>");
    /** number of attempts to replay query if recoverable http code 500 received */
     public IntegerOption numRetries=new IntegerOption("retry-max <number of attemts to replay query if deadlock error message received>", 3
             ,"default: %%");
     IntegerOption retryInterval_lowOpt=new IntegerOption("retry-int <time interval between attempts to replay query (milliseconds)>", 200
             ,"Increases by 1.5 times for each subsequent attempt."
             ,"default: %% ms");
     IntegerOption retryInterval_highOpt=new IntegerOption("retry-intmax <upper bound of time interval between attempts to replay query (milliseconds)>", 0
             ,"If set, actual retry-int is picked up randomly between set retry-int and retry-intmax"
             ,"default: equals to retry-int");

    protected String[] sparqlEndpoints = null;
    protected String[] sparqlUpdateEndpoints = null;
    private int updateEndpointIndex=0;
    private int endpointIndex=0;
    protected double scaleFactor;
    public AbstractParameterPool parameterPool;
    protected File qualOutFile=null;
    protected boolean dryMode=false;
    int retryInterval_low;
    int retryInterval_high;
    Random retryGen; // = new Random();

    public boolean isDryMode() {
        return dryMode;
    }

    public String[] getSparqlEndpoints() {
        return sparqlEndpoints;
    }

    public String[] getSparqlUpdateEndpoints() {
        return sparqlUpdateEndpoints;
    }

    public synchronized String getNextEndPoint(byte queryType) {
        String serviceURL;
        if (queryType==Query.UPDATE_TYPE) {
            serviceURL=sparqlUpdateEndpoints[(updateEndpointIndex++)%sparqlUpdateEndpoints.length];
        } else {
            serviceURL=sparqlEndpoints[(endpointIndex++)%sparqlEndpoints.length];
        }
        return serviceURL;
    }
    
    public synchronized int getRetryInt() {
        int diap=retryInterval_high-retryInterval_low;
        if (diap>0) {
            return retryInterval_low+retryGen.nextInt(diap+1);          
        } else {
            return retryInterval_low;
        }
    }
    
    public ConnectionParameters(String... usageHeader) throws IOException {
        super(usageHeader);
   }

    @Override
    protected void processProgramParameters(String[] args) {
        super.processProgramParameters(args);
        if (versionOpt.getValue()) {
            System.out.println(version);
            System.exit(0);
        }
        if (helpOpt.getValue()) {
            printUsageInfos();
            System.exit(0);
        }

        dryMode=dryRun.getValue();
        
        fixEndPoints();
        
        long seed = 808080L;
        retryGen = new Random(seed);
        retryInterval_low=this.retryInterval_lowOpt.getValue();
        retryInterval_high=this.retryInterval_highOpt.getValue();
        if (retryInterval_low>retryInterval_high) {
            if (retryInterval_high==0) { // just was not set
                retryInterval_high=retryInterval_low*2;
            } else  {
                DoubleLogger.getErr().println("invalid parameters: -retry-maxint < -retry-max\n");
                printUsageInfos();
                System.exit(-1);
            } 
        }
   }

    protected void fixEndPoints() {
        List<String> endpointsLoc = super.args;
        List<String> updateEndpointsLoc = updateEndpoints.getValue();

        String baseEndpoint = this.baseEndpoint.getValue();
        if (baseEndpoint != null) {
            for (int k = 0; k < endpointsLoc.size(); k++) {
                endpointsLoc.set(k, baseEndpoint +endpointsLoc.get(k));
            }
        }
        
        if (updateEndpointsLoc.size() == 0) {
            updateEndpointsLoc=endpointsLoc;
        } else if (baseEndpoint != null) {
            for (int k = 0; k < updateEndpointsLoc.size(); k++) {
                updateEndpointsLoc.set(k, baseEndpoint +updateEndpointsLoc.get(k));
            }
        }

        if (!dryMode && endpointsLoc.size() == 0 && updateEndpointsLoc.size() == 0) {
            DoubleLogger.getErr().println("No endpoints provided:\n");
            printUsageInfos();
            System.exit(-1);
        }

        this.sparqlEndpoints = endpointsLoc.toArray(new String[endpointsLoc.size()]);
        this.sparqlUpdateEndpoints = updateEndpointsLoc.toArray(new String[updateEndpointsLoc.size()]);
    }

}

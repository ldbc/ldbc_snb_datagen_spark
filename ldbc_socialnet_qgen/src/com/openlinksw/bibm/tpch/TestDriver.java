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
package com.openlinksw.bibm.tpch;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.AbstractTestDriver;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.QueryMix;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.Exceptions.RequestFailedException;
import com.openlinksw.bibm.qualification.Comparator;
import com.openlinksw.bibm.qualification.ResultCollector;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.FileUtil;
import com.openlinksw.util.FiniteQueue;

public class TestDriver extends AbstractTestDriver {
    StringOption scaleFactorOpt=new StringOption("scale <scale factor>", null
            ,"<scale factor> is the scale factor of the database being tested." 
            ,"This value is used only to calculate performance figures.");
    
    StringOption uscaseDirname=new StringOption("uc <use case query mix directory>", null
            ,"specifies the query mix directory.");
    
    public StringOption refreshProg1=new StringOption("rf1 <refresh command 1>", null
            ,"<refresh command> - the command to run the refresh function 1 (insert, see TPC-H spec)");
    public StringOption refreshProg2=new StringOption("rf2 <refresh command>", null
            ,"<refresh command 2> - the command to run the refresh function2  (delete, see TPC-H spec)");
    public StringOption user=new StringOption("user <database user name>", null);
    public StringOption password=new StringOption("password <database user's password>", null);

    File uscaseDir;
    QueryMix queryMix;
    int nClients;
    public FiniteQueue<AbstractQueryResult> resultQueue=new FiniteQueue<AbstractQueryResult>(new LinkedList<AbstractQueryResult>()); 
    protected Comparator cmp;
    protected ResultCollector collector;
    boolean success=true;
    public Properties connectionProperties=new Properties();
   
    public TestDriver(String args[]) throws IOException {
        super(version,
                "Usage: com.openlinksw.bibm.tpch.TestDriver <options> endpoints...", // CHECK package
                 "endpoint: The URL of the HTTP SPARQL or SQL endpoint");
        super.processProgramParameters(args);
        String tpchscale = scaleFactorOpt.getValue();
 
        if (tpchscale==null) {
            scaleFactor=1;
        } else {
            scaleFactor=Double.parseDouble(tpchscale);
        }

        // create ParameterPool
        boolean doSQL=this.doSQL.getValue();
        long seed = this.seed.getValue();
        char parameterChar=doSQL? '@' : '%';
        parameterPool = new ParameterPool(parameterChar, seed, scaleFactor);  

        // read query mixes
        uscaseDir = new File(queryRootDir.getValue(), uscaseDirname.getValue());
        if (!uscaseDir.exists()) {
            throw new BadSetupException("no such usecase directory: "+uscaseDir);
        }
        queryMix = new QueryMix(parameterPool, uscaseDir);

        int minClients= Math.max(2, (int)Math.round(Math.log10(scaleFactor)*2+1)); // see TPC-H spec 5.3.4
        if (nrThreads.getSetValue()==null) {
            nClients=minClients;
            DoubleLogger.getOut().println("Number of clients is set to ", minClients);
        } else {
            nClients=nrThreads.getValue();
            if (nClients<minClients) {
                DoubleLogger.getErr().println("Warning: TPC-H spec requires at least ", minClients, " clients for scale factor ", scaleFactor);
            }
        }
        if (user.getValue()!=null) {
            connectionProperties.setProperty("user", user.getValue());
        }
        if (user.getValue()!=null) {
            connectionProperties.setProperty("password", password.getValue());
        }
        DoubleLogger.getOut().println("..done");
  }
    
    @Override
    public Collection<Query> getQueries() {
        return queryMix.getQueries().values();
    }
   
    public void runQualificationTest() throws Exception {
        ClientManager manager = new ClientManager(this);

        File qualificationCompareFile = this.qualificationCompareFile.getValue();
        if  (qualificationCompareFile!=null) {
            cmp=new Comparator(qualificationCompareFile);
        }
        if  (qualOutFile!=null) {
            collector=new ResultCollector(qualOutFile);
            collector.addResultDescriptionsFromDriver(this);
       }

        manager.startQualificationTest();
        for (;;) {
            AbstractQueryResult result = resultQueue.take();
            if (result==null) {
                // finish
                break;
            }
            if (collector != null) {
                collector.addResult(result.getQueryResult());
            }
            if (printResults.getValue()) {
               result.logResultInfo(logger);
            }
            if (cmp!=null) {
                cmp.addQueryResult(result.getQueryResult());
            }
        }

        QueryMixStatistics queryMixStat = manager.getQueryMixStat();
        queryMixStat.fillFrame(true, queryMix.getQueries());
        String execSummary = queryMixStat.getResultString();
        logger.append(execSummary);
        logger.flush();
        FileUtil.strings2file(new File("report.txt"), execSummary); // TODO improve
        printXML(queryMixStat);
        if (cmp!=null) {
            cmp.reportTotal();
        }
        this.success=this.success &&manager. success;
    }

    public void runPerformanceTest() throws Exception {
        File qualificationCompareFile = this.qualificationCompareFile.getValue();
        if  (qualificationCompareFile!=null) {
            cmp=new Comparator(qualificationCompareFile);
        }
        ClientManager manager = new ClientManager(this);
        manager.startPowerTest();
        for (;;) {
            AbstractQueryResult result = resultQueue.take();
            if (result==null) {
                // finish
                break;
            }
            if (printResults.getValue()) {
                result.logResultInfo(logger);
             }
            if (cmp!=null) {
                cmp.addQueryResult(result.getQueryResult());
            }
        }

        resultQueue.reset();
        manager.startThroughputTest();
        for (;;) {
            AbstractQueryResult result = resultQueue.take();
//            DoubleLogger.getOut().println("res=", result);
            if (result==null) {
                // finish
                break;
            }
            if (printResults.getValue()) {
                result.logResultInfo(logger);
             }
            if (cmp!=null) {
                cmp.addQueryResult(result.getQueryResult());
            }
        }

        QueryMixStatistics queryMixStat = manager.getQueryMixStat();
        queryMixStat.fillFrame(true, queryMix.getQueries());
        String execSummary = queryMixStat.getResultString();
        logger.append(execSummary);
        logger.flush();
        FileUtil.strings2file(new File("report.txt"), execSummary); // TODO improve
        printXML(queryMixStat);
        if (cmp!=null) {
            cmp.reportTotal();
        }
        this.success=this.success &&manager. success;
    }

    public void run() throws Exception {
        if (qualMode) {
            runQualificationTest();
        } else {
            runPerformanceTest();
        }
    }

    protected void testDriverShutdown() {
        if (collector != null) {
            try {
                collector.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String argv[]) throws InterruptedException {
        TestDriver testDriver = null;
        int exitCode=1;
        boolean printST=true; // TODO turn off
        DoubleLogger err = DoubleLogger.getErr();
        try {
            testDriver = new TestDriver(argv);
            DoubleLogger.getOut().println("\nStarting test...\n");
            testDriver.run();
            if (testDriver.success) {
                exitCode=0;
            }
        } catch (ExceptionException e) {
            err.println(e.getMessages());
            if (printST || e.isPrintStack()) {
                 err.printStackTrace(e.getCause());
            }
        } catch (BadSetupException e) {
            err.println(e.getMessage());
            if (printST || e.isPrintStack()) {
                 err.printStackTrace(e);
            }
        } catch (RequestFailedException e) {
            if (e.getMessage()!=null) {
                err.println("Request failed: ", e.getMessage());              
            }
            if (printST) {
                err.printStackTrace(e);
            }
        } catch (Throwable e) {
            err.printStackTrace(e);
        } finally {
            if (testDriver==null) return;
            testDriver.testDriverShutdown();
            System.exit(exitCode);
        }
    }

}

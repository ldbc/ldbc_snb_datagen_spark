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
package com.openlinksw.bibm.tpchAcid;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;
import com.openlinksw.bibm.QueryMix;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.Exceptions.RequestFailedException;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Options;

public class TestDriver extends Options {
    public static final String version = "Openlink BIBM ACID Test Driver 0.2";

    public long startTime = System.currentTimeMillis();
    public Date date = new Date();
    DoubleLogger err = DoubleLogger.getErr();

    BooleanOption versionOpt = new BooleanOption("version", "prints the version of the Test Driver");
    BooleanOption helpOpt = new BooleanOption("help", "prints this help message");

    StringOption scaleFactorOpt = new StringOption("scale <scale factor>", null,
            "<scale factor> is the scale factor of the database being tested.");

    FileOption errFileName = new FileOption("err-log", null,
            "log file name to write error messages", "default: print errors only to stderr");

    public IntegerOption nrRuns = new IntegerOption("runs <number of query mix runs>", -1,
            "default: the number of clients");

    StringOption querymixDirNames = new StringOption("uc <use case query mix directory>", null,
            "Specifies the query mix directory.");
    /*
     * FileOption xmlResultFile=new
     * FileOption("o <benchmark results output file>", "benchmark_result.xml"
     * ,"default: %%"); public StringOption defaultGraph=new
     * StringOption("dg <default graph>", null
     * ,"add &default-graph-uri=<default graph> to the http request");
     */

    public IntegerOption nrThreads = new IntegerOption("mt <Number of clients>", 1,
            "Run multiple clients concurrently.", "default: 1");

    public LongOption seedOpt = new LongOption("seed <Long Integer>", 808080L,
            "Init the Test Driver with another seed than the default.", "default: %%");

    public IntegerOption timeout = new IntegerOption("t <timeout in ms>", 0, "Timeouts will be logged for the result report.", "default: %%");

    public StringOption driverClassName = new StringOption("dbdriver <DB-Driver Class Name>", "com.mysql.jdbc.Driver", 
            "default: %%");
    FileOption queryRootDir = new FileOption("qrd <query root directory>", ".",
            "Where to look for the directoried listed in the use case file.",
            "default: current working directory");

    public StringOption retryErrorMessage = new StringOption("retry-msg <message from server indicating deadlock>", null // for
                                                                                                                         // Virtuoso,
                                                                                                                         // "40001";
            , "default: <null, that does not match any string>");
    /** number of attempts to replay query if recoverable http code 500 received */
    public IntegerOption numRetries = new IntegerOption("retry-max <number of attemts to replay query if deadlock error message received>", 3,
            "default: %%");
    IntegerOption retryInterval_lowOpt = new IntegerOption("retry-int <time interval between attempts to replay query (milliseconds)>", 200,
            "Increases by 1.5 times for each subsequent attempt.", "default: %% ms");
    IntegerOption retryInterval_highOpt = new IntegerOption("retry-intmax <upper bound of time interval between attempts to replay query (milliseconds)>", 0,
            "If set, actual retry-int is picked up randomly between set retry-int and retry-intmax", "default: equals to retry-int");

    int retryInterval_low;
    int retryInterval_high;

    String endPoint = null;

    double scaleFactor;
    File  querymixDir;
    private String[] queryNames;

    protected Random rand;

    public String getEndpoint() {
        return endPoint;
    }

    /*
     * Process the program parameters typed on the command line.
     */
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

        String tpchscale = scaleFactorOpt.getValue();
        if (tpchscale == null) {
            err.println("Warning: scale factor is not set. Using default value 1.");
            scaleFactor = 1d;
        } else {
            scaleFactor = Double.parseDouble(tpchscale);
        }
        if (nrRuns.getValue() < 0) {
            nrRuns.setValue(nrThreads.getValue());
        }

        retryInterval_low = this.retryInterval_lowOpt.getValue();
        retryInterval_high = this.retryInterval_highOpt.getValue();
        if (retryInterval_low > retryInterval_high) {
            if (retryInterval_high == 0) { // just was not set
                retryInterval_high = retryInterval_low * 2;
            } else {
                err.println("invalid parameters: -retry-maxint < -retry-max\n");
                printUsageInfos();
                System.exit(-1);
            }
        }

        List<String> endpointsLoc = super.args;

        if (endpointsLoc.size() != 1) {
            err.println("Exactly one endpoint must be provided! (got " + endpointsLoc.size() + ")\n");
            printUsageInfos();
            System.exit(-1);
        }

        this.endPoint = endpointsLoc.get(0);

        try {
            Class.forName(driverClassName.getValue());
        } catch (ClassNotFoundException e) {
            throw new ExceptionException("Driver class not found:", e);
        }
    }

    public TestDriver(String args[]) throws IOException {
        super(version, "Usage: com.openlinksw.ibm.tpchAcid.TestDriver <options> endpoints...", // CHECK package
                "endpoint: The URL of the HTTP SPARQL or SQL endpoint");
        processProgramParameters(args);
        long seed = this.seedOpt.getValue();
        rand = new Random(seed);

        // read query mix
        String uscaseDirname = this.querymixDirNames.getValue();
        if (uscaseDirname != null) {
            querymixDir = new File(queryRootDir.getValue(), uscaseDirname);
            QueryMix queryMix = new QueryMix(querymixDir);
            this.queryNames = queryMix.getQueryNames();
        } else {
            this.queryNames = new String[] { "a1", "a2", "c1", "i1", "i2", "i3", "i4", "i5", "i6"};
        }

        DoubleLogger.getOut().println("..done");
    }

    // ****** runtime support *******//
    protected static Logger logger = Logger.getLogger(TestDriver.class);
    HashMap<String, AcidTest> drivers = new HashMap<String, AcidTest>();

    public void run() throws Exception {
        DoubleLogger out = DoubleLogger.getOut();
        for (String queryName : queryNames) {
            String id = queryName.substring(0, 1);
            int num = Integer.parseInt(queryName.substring(1));
            AcidTest driver=null;
            switch (id.charAt(0)) {
            case 'a':
                switch (num) {
                case 1:
                    driver= new AtomicityTest1(this);
                    break;
                case 2:
                    driver= new AtomicityTest2(this);
                    break;
                }
                break;
            case 'c':
                switch (num) {
                case 1:
                    driver= new ConsistencyTest1(this);
                    break;
                }
                break;
            case 'i':
                switch (num) {
                case 1:
                    driver= new IsolationTest1(this);
                    break;
                case 2:
                    driver= new IsolationTest2(this);
                    break;
                case 3:
                    driver= new IsolationTest3(this);
                    break;
                case 4:
                    driver= new IsolationTest4(this);
                    break;
                case 5:
                    driver= new IsolationTest5(this);
                    break;
                case 6:
                    driver= new IsolationTest6(this);
                    break;
                }
                break;
           }
            if (driver == null) {
                err.println("Wrong query name: " + queryName);
                continue;
            }
            try {
                String err = driver.runTest();
                if (err == null) {
                    out.println("Test " + queryName + " passed");
                } else {
                    out.println("Test " + queryName + " failed:" + err);
                }
            } catch (Exception e) {
                out.println("Test " + queryName + " failed:" + e.toString());
                out.printStackTrace(e);
            }
        }

    }

    public static void main(String argv[]) throws InterruptedException {
        TestDriver testDriver = null;
        boolean printST = true; // TODO turn off
        DoubleLogger err = DoubleLogger.getErr();
        try {
            testDriver = new TestDriver(argv);
            DoubleLogger.getOut().println("\nStarting tests ...\n");
            testDriver.run();
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
            if (e.getMessage() != null) {
                err.println("Request failed: ", e.getMessage());
            }
            if (printST) {
                err.printStackTrace(e);
            }
        } catch (Throwable e) {
            err.printStackTrace(e);
        } finally {
        }
    }
}

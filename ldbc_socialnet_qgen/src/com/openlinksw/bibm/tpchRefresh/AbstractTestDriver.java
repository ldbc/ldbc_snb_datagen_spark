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
package com.openlinksw.bibm.tpchRefresh;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Options;

public abstract class AbstractTestDriver extends Options {
    public static final String version = "Openlink BIBM TPCH Refresh Test Driver 0.1";

    public long startTime = System.currentTimeMillis();
    public Date date = new Date();

    BooleanOption versionOpt = new BooleanOption("version", "prints the version of the Test Driver");
    BooleanOption helpOpt = new BooleanOption("help", "prints this help message");

    FileOption errFileName = new FileOption("err-log", null,
            "log file name to write error messages", "default: print errors only to stderr");

    StringOption updateDirName = new StringOption("udd <update data directory>", null,
            "Specifies the update dataset directory.");
    public IntegerOption dsN = new IntegerOption("ds <dataset number>", null, "The number of the dataset to use.");

    public IntegerOption timeout = new IntegerOption("t <timeout in ms>", 0, "Timeouts will be logged for the result report.", "default: %%");

    public StringOption driverClassName = new StringOption("dbdriver <DB-Driver Class Name>", "com.mysql.jdbc.Driver", 
            "default: %%");

    String endPoint = null;
    File updateDir;

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

        try {
            Class.forName(driverClassName.getValue());
        } catch (ClassNotFoundException e) {
            fatal("Driver class not found:"+driverClassName.getValue());
        }
        
        List<String> endpointsLoc = super.args;
        if (endpointsLoc.size() != 1) {
            fatal("Exactly one endpoint must be provided! (got " + endpointsLoc.size() + ")\n");
        }
        this.endPoint = endpointsLoc.get(0);

        String uddName = this.updateDirName.getValue();
        if (uddName == null) {
            fatal("No dataset directory provided. Exiting");
        }
        updateDir = new File(uddName);
        if (!updateDir.exists()) {
            fatal(uddName+" does not exists");
        }
        if (!updateDir.isDirectory()) {
            fatal(uddName+" is not a directory");
        }
        if (this.dsN.getValue() == null) {
            fatal("No dataset number provided. Exiting");
        }

    }

    public AbstractTestDriver(String... usageHeader) throws IOException {
        super(usageHeader);
    }
    
    public void fatal(String message) {
        DoubleLogger.getErr().println(message);
        System.exit(-1);
    }


}

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

import static com.openlinksw.util.FileUtil.strings2file;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Random;
import com.openlinksw.bibm.statistics.AbstractQueryMixStatistics;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Exec;

public abstract class AbstractTestDriver extends ConnectionParameters {

    public long startTime=System.currentTimeMillis();
    public Date date=new Date();

   FileOption xmlResultFile=new FileOption("o <benchmark results output file>", "benchmark_result.xml"
           ,"default: %%");
   
   public LongOption seed=new LongOption("seed <Long Integer>", 808080L
           ,"Init the Test Driver with another seed than the default."
           ,"default: %%");
   
   public IntegerOption timeout=new IntegerOption("t <timeout in ms>", 0
           ,"Timeouts will be logged for the result report."
           ,"default: "+0);
   
   StringOption sutCmd=new StringOption("sut <sutcommand>", null
           ,"Measures the server's CPU time using external program."
           ,"<sutcommand> - the command to run external program, arguments delimited with comma");
   
   private static final String def_qualificationFile = "run.qual";
   protected BooleanOption qualification=new BooleanOption("q"
           ,"generate output qualification file with the default name ("+def_qualificationFile+")");
   public FileOption qualificationFile=new FileOption("qf <output qualification file name>", def_qualificationFile
           ,"generate output qualification file with this name");
   protected FileOption qualificationCompareFile=new FileOption("qcf <input qualification file name>", null
           ,"To turn on comparison of resultst."
           ,"default: none.");

   protected FileOption queryRootDir=new FileOption("qrd <query root directory>", "."
           ,"Where to look for the directoried listed in the use case file."
           ,"default: current working directory");
   
    public BooleanOption useDefaultParams=new BooleanOption("defaultparams"
            ,"use default query parameters ");

    protected BooleanOption printResults=new BooleanOption("printres"
            ,"include results into the log");
    
    protected double scaleFactor;
    public AbstractParameterPool parameterPool;
    protected File qualOutFile=null;
    protected boolean qualMode=false;
    int retryInterval_low;
    int retryInterval_high;
    Random retryGen; // = new Random();
    protected FileWriter logger;

    public AbstractTestDriver(String... usageHeader) throws IOException {
        super(usageHeader);
        File logFile = new File("run.log");
        DoubleLogger.setErrFile(logFile);
        DoubleLogger.setOutFile(logFile);
        logger = new FileWriter(logFile);
   }

    public abstract  Collection<Query>getQueries();
    
    public boolean isQualMode() {
        return qualMode;
    }

    public synchronized int getRetryInt() {
        int diap=retryInterval_high-retryInterval_low;
        if (diap>0) {
            return retryInterval_low+retryGen.nextInt(diap+1);          
        } else {
            return retryInterval_low;
        }
    }
    
    @Override
    protected void processProgramParameters(String[] args) {
        super.processProgramParameters(args);
        DoubleLogger out = DoubleLogger.getOut();
        out.println("Reading Test Driver data...");

        if  (qualification.getValue() || qualificationFile.getSetValue()!=null) {
            qualOutFile=qualificationFile.getSetValue();
            if (qualOutFile==null) { // only construct new file name for default
                qualOutFile=qualificationFile.newNumberedFile();
            }
            out.println("Result data will be written in "+qualOutFile.getAbsolutePath());
            qualMode=true;
            if (!useDefaultParams.getValue()) {
                out.println("Warning: option -defaultparams is not set. Result data will not be comparable.");
            }
       }

        if (super.isDryMode()) {
            printResults.setValue(true);
        }
        out.flush();
    }

    //***** SUT time *****//
   
    private Double sutStartTime;
    private Double sutEndTime;

    public void sutStart() {
        String sutCmd = this.sutCmd.getValue();
        if (sutCmd==null) return;
        sutStartTime=sutCurrent(sutCmd);      
    }

    public void sutEnd() {
        String sutCmd = this.sutCmd.getValue();
        if (sutCmd==null) return;
        sutEndTime=sutCurrent(sutCmd);        
    }
    
    public Double getSUT() {
        if (sutStartTime==null || sutEndTime==null) {
            return null;
        }
        return sutEndTime-sutStartTime;
    }

    private Double sutCurrent(String sutCmd) {
        String[] lines=null;
        try {
            lines=Exec.execProcess(sutCmd.split(","));
        } catch (Exception e) {
            e.printStackTrace();
        }
        double res=0;
        if (lines==null) {
            DoubleLogger.getErr().println("sutCurrent: lines=null");
            return null;
        }
        for (String line: lines) {
            String[] tokens=line.split(":"); // hh:mm:ss
            if (tokens.length!=3) {
                DoubleLogger.getErr().println("sutCurrent: wrong line from sut command: "+line);
                return null;
            }
            double time=Double.parseDouble(tokens[0])*3600+Double.parseDouble(tokens[1])*60+Double.parseDouble(tokens[2]);
            res+=time;
        }
        return res;
    }
    
   /**
     * if xmlResultFile exists, print to xmlResultFileName.(N+1).xml
     * @param queryMixStat 
     */
    protected void printXML(AbstractQueryMixStatistics queryMixStat) {
        File resFile=xmlResultFile.newNumberedFile();
        String xml=queryMixStat.toXML();
        strings2file(resFile, xml);
    }

}

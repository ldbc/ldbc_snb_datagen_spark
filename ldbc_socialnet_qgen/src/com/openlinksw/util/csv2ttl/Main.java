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
package com.openlinksw.util.csv2ttl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.lang.*;

import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.ConnectionParameters;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.connection.NetQuery;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Util;

public class Main  extends ConnectionParameters implements Runnable {
    private static final int MAX_FILE_BUFFER_SIZE = 64*1024;
	private static final String defaultExt= "csv";

    DoubleLogger err=DoubleLogger.getErr();
    DoubleLogger out=DoubleLogger.getOut();
	String ext;
	char delimeter;
	String defaultGraph;
	DBSchema dbSchema;
    ArrayDeque<File>sourceFiles=new ArrayDeque<File>();
	File destDir;
	int triplesPerQuery;
	int doSplitAt;
	boolean doGzip=false;
	
	public Main(String[] args) throws Exception {
	    super("Usage: com.openlink.util.csv2ttl.Csv2ttl [options]... [sourcefiles]...",
	            "  sourcefiles can be directories");
	    StringOption extOpt=new StringOption("ext <input file extention> (to search in souce directories)", defaultExt
	            ,"default: '"+defaultExt+"'"
	            );
        StringOption delimeter=new StringOption("delim <field delimeter>",  "|"
                ,"default: %%");
	    StringOption schema=new StringOption("schema <conversion schema> (json file)", null);
        StringOption destDirName=new StringOption("d <destination directory>", "."
                , "default: current working directory");
        IntegerOption triplesPerQuery=new IntegerOption("tpq <triples per query>", 500
                , "default: %%");
        IntegerOption doSplitAt=new IntegerOption("split <triples per file>", 0, "split output files on N triples boundary.");
        BooleanOption doGzip=new BooleanOption("gz", "compress output files with gzip format");

        super.processProgramParameters(args);

        this.ext=extOpt.getValue();
        this.delimeter=delimeter.getValue().charAt(0);
        this.defaultGraph=super.defaultGraph.getSetValue();
        this.triplesPerQuery=triplesPerQuery.getValue();
        this.doSplitAt = doSplitAt.getValue();
        String schemaName = schema.getValue();
        if (schemaName==null) {
            fatal("No schema provided. Exiting. ");
        }
        try {
            dbSchema=new DBSchema(schemaName);
        } catch (Exception e) {
            err.println("Problems loading schema file "+schemaName);
            err.println(e.getMessage());
            throw e;
        }

        ArrayList<File>sourceFiles=new ArrayList<File>();
        for (String arg: super.args) {
                File f=new File(arg);
                if (!f.exists()) {
                    fatal("file not exists: "+f.getAbsolutePath());
                }
                if (f.isDirectory()) {
                    String ext1="."+ext;
                    String ext2=ext1+".";
                    for (String fn: f.list()) {
                        if (!(fn.endsWith(ext1)||fn.contains(ext2))) continue;
                        File ff=new File(f, fn);
                        if (ff.isDirectory()) continue;
                        sourceFiles.add(ff);
                    }
                } else {
                    sourceFiles.add(f);
                }
        }
        if (sourceFiles.size()==0) {
            err.println("No source files. Exiting. ");
            System.exit(-1);
        }
        sortSources(sourceFiles.toArray(new File[sourceFiles.size()]));
        
        if (sparqlUpdateEndpoints.length==0) { // file convertion or direct loading?
            destDir=new File(destDirName.getValue());
            if (!destDir.exists()) {
                destDir.mkdirs();
                if (!destDir.exists()) {
                    fatal("Cannot create destination directory: "+destDir.getAbsolutePath());
                }
            }
            this.doGzip=doGzip.getValue();
        }

        
	}

    @Override
   protected void fixEndPoints() {
        List<String> updateEndpointsLoc = updateEndpoints.getValue();

        String baseEndpoint = this.baseEndpoint.getValue();
        if (baseEndpoint != null) {
            for (int k = 0; k < updateEndpointsLoc.size(); k++) {
                updateEndpointsLoc.set(k, baseEndpoint +updateEndpointsLoc.get(k));
            }
        }

        this.sparqlUpdateEndpoints = updateEndpointsLoc.toArray(new String[updateEndpointsLoc.size()]);
    }

    private void fatal(String message) {
        err.println(message);
        System.exit(-1);
    }

    public synchronized String getNextEndPoint() {
        return super.getNextEndPoint(Query.UPDATE_TYPE);
    }

    private void sortSources(File[] files) {
		Comparator<File> comparat=new Comparator<File>(){

			@Override
			public int compare(File o1, File o2) {
				long size1 = o1.length();
				long size2 = o2.length();
				return size1>size2?+1:(size1<size2?-1:0);
			}
			
		};
//		Arrays.sort(files, comparat);  - no improvement, to be investigated
		for (File file: files) {
			sourceFiles.add(file);
		}
	}

	synchronized File getNextTask() {
		File f = sourceFiles.poll();
		return f;
	}

	int maxFailureCount = 10;
	ArrayList<String> failures = new ArrayList<String>();

	synchronized boolean reportFailure(String msg) {
		failures.add(msg);
		return failures.size()>=maxFailureCount;
	}
	
	void checkFailures() {
		if (failures.size()==0) return;
		err.println("Some file convertions failed:");
		for (int k=0; k<failures.size(); k++) {
			err.println(failures.get(k));
		}
	}

	void runAll() throws IOException {
        long start=System.currentTimeMillis();
        int availableProcessors = Runtime.getRuntime().availableProcessors();
		Integer mt=super.nrThreads.getSetValue();
		int nThreads=mt==null?Math.min(availableProcessors, sourceFiles.size()):mt;
		Thread[] threads=new Thread[nThreads];
		for (int k=0; k<nThreads; k++) {
			Thread thread = new Thread(this);
			threads[k]=thread;
			thread.start();
		}
		try {
			for (int k=0; k<nThreads; k++) {
				threads[k].join();
			}
		} catch (InterruptedException e) {
			for (int k=0; k<nThreads; k++) {
				threads[k].interrupt();
			}
		}
        long end=System.currentTimeMillis();
        out.println("Exec time="+(end-start)/1000+" sec");
	}

    @Override
    public void run() {
        for (;;) {
            File src = getNextTask();
            if (src==null) return;
            try {
                String srcFileName=src.getName();
                String[] parts=srcFileName.split("\\.");
                if (parts.length<2 || !parts[1].equals(ext)) {
                    // not an error, probably we are passed something like"data/*" 
                    // err.println("Source file name, skipped: "+destFileName);              
                    out.println("file "+src+" is not a ."+ext);
                    return;
                }
                String tableName=parts[0];
                CsvTableSchema tableSchema=dbSchema.getTable(tableName);
                if (tableSchema==null) {
                    out.println("no schema for "+ext+" file:"+src);
                    return;
                }
                if (sparqlUpdateEndpoints.length>0) {
                    loadFile(src, tableSchema, getNextEndPoint());
                } else {
                    String fileNumber=(parts.length==2)? "": "."+parts[2];
                    String destFileName=tableName+fileNumber+".ttl";
                    convertFile(src, tableSchema, destFileName);
                }
            } catch (IOException e) {
                if (reportFailure(e.getMessage())) {
                    return;
                }
            }
        }
    }

	void loadFile(File src, CsvTableSchema tableSchema, String nextEndPoint) throws IOException {
        out.println("start loading file "+src+"...");
        CsvConverter reader=new CsvConverter(src, delimeter, dbSchema, tableSchema);
        int recN=0;
        int errCount=0;
        boolean has_records=true;
        while (has_records) {
            StringBuilder sb = new StringBuilder();
            int lineCount=0;
            sb.append(dbSchema.getUpdate_header_str());
            sb.append("INSERT DATA {\n");
            for (;;) {
                if (!reader.readRecord()) {
                    has_records=false;
                    break;
                }
                String values = reader.getNodeString();
                recN++;
                if (values == null) {
                    if (errCount++ < 10) {
                        err.println("bad record " + recN + ", skipped");
                    } else {
                        err.println("bad record " + recN + ", exiting.");
                        return;
                    }
                } // else it may contain excessive fields, so use fields.length
                sb.append(values);
                lineCount+=Util.countLines(values);
                if (lineCount>=triplesPerQuery) {
                    break;
                }
            }
            sb.append("}\n");
            CompiledQuery query = new CompiledQuery("load", Query.UPDATE_TYPE, sb.toString());
            NetQuery nQuery = new NetQuery(query, this);
            try {
                nQuery.exec();
            } catch (InterruptedException e) {
                throw new ExceptionException("nQuery.exec():", e);
            }
        }
        out.println("  ... file "+src+" loaded");
    }

	OutputStream makeOutputStream(String destFileName, int fileNo) throws IOException 
	{
	    OutputStream os;
	    if (fileNo > 0)
	    	destFileName = destFileName + "." + java.lang.Integer.toString(fileNo);
	    if (doGzip) {
	        File dest=new File(destDir, destFileName+".gz");
	        os = new GZIPOutputStream(new FileOutputStream(dest), MAX_FILE_BUFFER_SIZE);
	    } else {
	        File dest=new File(destDir, destFileName);
	        os=new BufferedOutputStream(new FileOutputStream(dest), MAX_FILE_BUFFER_SIZE);
	    }
	    return os;
	}
	
    void convertFile(File src, CsvTableSchema tableSchema, String destFileName) throws IOException {
        out.println("start converting file "+src+"...");
        CsvConverter reader=new CsvConverter(src, delimeter, dbSchema, tableSchema);
        // create dest file name: dir/table.ext.n => table.n.ttl
        OutputStreamWriter writer = new OutputStreamWriter(makeOutputStream(destFileName, 0));
        writer.write(dbSchema.getHeader_str());
		int recN=0;
		int recNoInFile=0;
		int errCount=0;
		int fileNo=0;
		while (reader.readRecord()) {
			if (doSplitAt > 0 && recNoInFile >= doSplitAt)
			{
				writer.close();
				++fileNo;
				recNoInFile = 0;
				writer = new OutputStreamWriter(makeOutputStream(destFileName, fileNo));
				writer.write(dbSchema.getHeader_str());
			}
			++recN;
			++recNoInFile;
			String values = reader.getNodeString();
			if (values==null) {
				if (errCount++ <10) {
					err.println("bad record "+recN+", skipped");
				} else {
					err.println("bad record "+recN+", exiting.");
					break;
				}
			} // else it may contain excessive fields, so use fields.length
			writer.write(values);
		}
		writer.close();
		out.println("  ... file "+src+" converted");
	}

    public static void main(String[] args) throws Exception {
		Main csv2ttl;
        try {
            csv2ttl = new Main(args);
            csv2ttl.runAll();
            csv2ttl.checkFailures();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}

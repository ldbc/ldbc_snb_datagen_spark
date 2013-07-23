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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import com.csvreader.CsvReader;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.qualification.QualificationDataParser;
import com.openlinksw.bibm.qualification.QueryResult;
import com.openlinksw.bibm.qualification.QueryResultAssembler;
import com.openlinksw.bibm.qualification.ResultCollector;
import com.openlinksw.bibm.qualification.QualificationDataParser.NamedResultDescriptionLists;
import com.openlinksw.bibm.qualification.QualificationDataParser.ResultDescriptors;
import com.openlinksw.bibm.qualification.QueryResult.Row;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Nameable;

public class ValidTPCH extends ResultCollector  {
    static final int MAX_FILE_BUFFER_SIZE = 64*1024;
    ResultDescriptors resultDescriptors;
    HashMap<String, QueryResult> qResults=new HashMap<String, QueryResult>();
        
    public ValidTPCH(File resultFile, File baseFile, File answersDir) throws IOException {
        super(resultFile);
        comment(" Valid TPC-H answers");
        addEntry("date", "'"+new Date().toString()+"'");
        
        ResultDescriptorsCopier rdp=new ResultDescriptorsCopier();
        rdp.loadFrom(baseFile);
        
        super.startResultDescriptors();
        resultDescriptors = rdp.resultDescriptors;
        for (Nameable namedResultDescriptor: resultDescriptors) {
           walk(namedResultDescriptor);
           endElement();
        }
        super.endResultDescriptors();

        loadAllResults(answersDir);
        close();
    }

    void loadAllResults(File answersDir) throws IOException {
        startAllResults();
        for (int qN=1; qN<=22; qN++) {
            String fname="q"+qN+".out";
            File src = new File(answersDir, fname);
            if (src.exists()) {
                loadResult(src, Integer.toString(qN));
            }
        }
    }
    
    void loadResult(File src, String qname) throws IOException {
        NamedResultDescriptionLists rd = resultDescriptors.get(qname);
        QueryResult queryResult = qResults.get(qname);
        if (queryResult==null) {
            DoubleLogger.getOut().println("queryResult =null qname=", qname);
            return;
        }
        Row params = queryResult.getParams();
        String[] stringParams=params.toArray(new String[params.size()]);
        QueryResultAssembler as= new QueryResultAssembler(rd.getResultDescriptions(), this);

        startObject();
        addEntry("queryName", qname);        
        addEntry("nRun", "1");        

        as.startHeader();
        Row header = queryResult.getHeader();
        for (Object el: header) {
            walk(el);
            endElement();
         }
        as.endHeader();

        as.appendParams(stringParams);
        
        BufferedReader freader = new BufferedReader(new FileReader(src) , MAX_FILE_BUFFER_SIZE);
        CsvReader reader=new CsvReader(freader, '|');
        reader.readRecord();      // header
        String[] values=reader.getValues();
        // actual format may vary, adjust parameters
        int first="".equals(values[0])?1:0; // skip data column started with delimeter
        int minus="".equals(values[values.length-1])?1:0; // skip data column started with delimeter
        int length=values.length-first-minus;
        as.startResults();
        for (int recN=0; reader.readRecord(); recN++) {
            values = reader.getValues();
            if ((values.length-first-minus)!=length) continue; // not a data line, skip
            as.startRow();
            for (int k=first; k<values.length-minus; k++) {
                as.addSqlRowElement(values[k]);
            }
            as.endRow();
        }
        as.endResults();
        
        super.endElement();

    }

    //===========================//
    
    class ResultDescriptorsCopier extends QualificationDataParser {

        @Override
        public void addQueryResult(QueryResult result) {
            qResults.put(result.getQueryName(), result);
        }
    }
    
    /*===============================*/
    public static void main(String[] args) throws IOException {
        if (args.length<3) {
            System.err.println("Usage: ValidTPCG <res file> <defaultparams.qual> <tpch answers dir>" );
            System.exit(1);
        }
        String resFileName=args[0];
        String paramNames=args[1];
        String answersName=args[2];
        try {
            new ValidTPCH(new File(resFileName), new File(paramNames), new File(answersName));
        } catch (ExceptionException e) {
            Throwable cause = e.getCause();
            System.err.println(cause.toString());
            if (e.isPrintStack()) {
                cause.printStackTrace();
           }
       } catch (BadSetupException e) {
           System.err.println("Bad setup:"+e.getMessage());
           if (e.isPrintStack()) {
                e.printStackTrace();
           }
        }
    }
}

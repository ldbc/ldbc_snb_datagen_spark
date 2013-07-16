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
package com.openlinksw.bibm.qualification;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.openlinksw.bibm.qualification.QueryResult.Results;
import com.openlinksw.bibm.qualification.QueryResult.Row;
import com.openlinksw.util.DoubleLogger;

public class Comparator extends QualificationDataParser {
    boolean fullCheck=false;
    String checkedFileName;
    DoubleLogger reportStream=DoubleLogger.getOut();
    ArrayList<String> failures=new ArrayList<String>();
    int failCount=0;
    int passCount=0;
    ValidDataParser validDataParser;
    private ResultDescription defaultResultDescription=ResultDescription.stringResult;
        
    public Comparator(String[] args) throws IOException {
        if (args.length<2) {
            System.err.println("Usage: Comparator [-full] <base.qual> <checked.qual>" );
            System.exit(1);
        }
        boolean fullCheckLoc=false;
        int k=0;
        if (args[0].equals("-full")) {
            fullCheckLoc=true;
            k++;
        }
        String baseFileName=args[k++];
        checkedFileName=args[k++];
        loadValidData(new File(baseFileName));
        fullCheck=fullCheckLoc; // deferred to avoid full report for loadValidData
    }

    public Comparator(File baseFile) throws IOException {
        loadValidData(baseFile);
    }

    public void loadValidData(File baseFile) throws IOException {
        validDataParser = new ValidDataParser();
        validDataParser.loadFrom(baseFile); //  load Base
        if (!validDataParser.baseResConsustent) {
            reportFailure("Base results are not consistent; exiting");
            reportFailureS("loading "+baseFile+" failed:");
        } else {
           int count = validDataParser.validResults.size();
           reportFailureS("loading "+baseFile+" successful; "+count+" results loaded.");
        }
    }

    void run() throws IOException {
        loadFrom(checkedFileName); //    compareChecked();
        reportTotal();
    }

    //=========================== Reporting //

    void reportFailure(String message) {
        failures.add(message);
    }

    void reportFailureS(String header) {
        reportStream.println(header);
        for (String message: failures) {
            reportStream.println("  ", message);
        }
        if (failures.size()>0) {
            failures=new ArrayList<String>();
        }
    }

    private void reportInconsistency(QueryResult result, QueryResult oldResult) {
        if (failures.size()>0) {
            reportFailureS("result for:"+result.getKey()+"in run:"+result.nRun+" is inconsistent with result in run:"+oldResult.nRun+":");
        }
    }
    
    private void reportQueryResult(QueryResult result) {
        if (failures.size()>0) {
            reportFailureS("***FAILED:"+result.getKey()+" in run "+result.nRun+" :");
            failCount++;
        } else {
            reportFailureS("PASSED: "+result.getKey()+" in run "+result.nRun);
            passCount++;
        }
    }
    
    public void reportTotal() {
        reportStream.println("Total: ", (failCount+passCount), " Passed: ", passCount, " Failed: ", failCount);
    }

    //=========================== Comparison //

    boolean compareResultSets(QueryResult validQR, QueryResult checkedQR) {
//        ResultDescription[] resultDescriptions = validQR.getRowDescr(); // TODO compare with checkedQR.rowDescr
        ResultDescription[] resultDescriptions = checkedQR.getRowDescr(); 
        Results valid = validQR.results;
        Results checked = checkedQR.results;
        
        int resultSetSize = valid.size();
        int checkedSize = checked.size();
        if (resultSetSize!=checkedSize) {
            reportFailure("resultset sizes differ: expected "+resultSetSize+" but was: "+checkedSize);
            return false;
        }
        
        HashMap<String, Row> resultMap = validQR.getResultMap();
        // simple comparison with the same order of results
        for (int k=0; k<resultSetSize; k++) {
            Row chRow=checked.get(k);
            Row vRow;
            if (resultMap==null) {
                vRow=valid.get(k);
            } else {
                vRow=resultMap.get(chRow.createKey());
                if (vRow==null) {
                    reportFailure("row "+(k+1)+" has no matching row in valid rowset. key = [" + chRow.createKey() + "]");
                    return false;
                }
            }
            if (!compareRows(k, resultDescriptions, vRow, chRow)) {
                return false;
            }
        }
        return true;
        
    }

    boolean compareRows(int rowNumber, ResultDescription[] resultDescriptions, Row vRow, Row chRow) {
        int rowSize = vRow.size();
        if (rowSize!=chRow.size()) {
            reportFailure("row sizes differ: expected "+rowSize+" but was: "+chRow.size());
            return false;
        }
        for (int k=0; k<rowSize; k++) {
            ResultDescription resultDescription;
            if (resultDescriptions!=null) {
                resultDescription = resultDescriptions[k];
            } else {
                resultDescription=null;
            }
            if (resultDescription==null) {
                resultDescription = defaultResultDescription;
            }
            String vValue=vRow.get(k);
            String chValue=chRow.get(k);
            if (!resultDescription.compare(vValue, chValue)) {
                reportFailure("value in row:"+(rowNumber+1)+" column:"+(k+1)+"("+resultDescription.get("column")+") does not match: expected \n["+vValue+"]\n but was: \n["+chValue+"]");
                if (!fullCheck) {
                    return false;
                }
            }
        }
        return true;
    }

    /*===============================*/
    
    @Override
    public void addQueryResult(QueryResult result) {
        QueryResult validResult = validDataParser.validResults.get(result.getKey());
        if (validResult==null) {
            return; // nothing to compare
        }
        compareResultSets(validResult, result);
        reportQueryResult(result);
    }

    @Override
    public void put(String key, Object value) {
        super.put(key, value);
        if (key.equals("resultDescriptors")) {
            compareResultDescriptors();
        }
    }

    private void compareResultDescriptors() {
        // TODO Auto-generated method stub
        //(resultDescriptors, validDataParser.resultDescriptors)
    }

    //===========================//

    class ValidDataParser extends QualificationDataParser {
        HashMap<String, QueryResult> validResults=new HashMap<String, QueryResult>();
        boolean baseResConsustent=true;

        @Override
        public void addQueryResult(QueryResult result) {
            String key = result.getKey();
            QueryResult oldResult = validResults.get(key);
            if (oldResult==null) {
                validResults.put(key, result);
            } else {
                if (!compareResultSets(oldResult, result)) {
                    reportInconsistency(result, oldResult);
                    baseResConsustent=false;
                }
           }
        }

    }
    
    //===========================//

    public static void main(String[] args) throws IOException {
        Comparator cmp=new Comparator(args);
        cmp.run();
    }

}

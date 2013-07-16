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

import java.util.HashMap;

import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.qualification.QualificationDataParser.NamedResultDescriptionLists;
import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonStack;
import com.openlinksw.util.json.ParserException;
import com.openlinksw.util.json.impl.AutoJsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.Sequence;
import com.openlinksw.util.json.impl.SimpleJsonList;

/**  the list of fields to be printed   */
@Sequence(value={"queryName", "nRun", "header", "params", "results"})
@IgnoreCase
public     class QueryResult extends AutoJsonObject implements Iterable<String>{    
    public String queryName;
    public String nRun;
    Row header;
    Row params;
    public Results results; 
    protected ResultDescription[] rds;
    protected Integer[] resultKeys;
    
    public QueryResult() {
    }

    public QueryResult(AbstractQueryResult aresult) {
        rds=aresult.getRds();
        resultKeys=aresult.getQuery().getQuery().getResultKeys();
    }

    public QueryResultAssembler getQueryResultAssembler(String queryName, int nRun) {
        JsonStack printer=new JsonStack(this, true);
        printer. startObject();
        printer.addEntry("queryName", queryName);        
        printer.addEntry("nRun", String.valueOf(nRun));        
        QueryResultAssembler as= new QueryResultAssembler(rds, printer);
        return as;
    }

    /**
     * @return unique key for compiled query
     */
    public String getKey() {
        StringBuilder sb=new StringBuilder();
        sb.append(queryName);
        for (String param: params) {
            sb.append(',').append(param);
        }
        return sb.toString();
    }
    
    @Override
    public JsonList<?> newJsonList(String key) {
       if ("header".equalsIgnoreCase(key)) {
            return new Row();
        } else if ("params".equalsIgnoreCase(key)) {
            return new Row();
        } else if ("results".equalsIgnoreCase(key)) {
            return new Results();
        } else {
            throw new ParserException("Not a List field:"+key);
        }
    }

    @Override
    public Iterable<String> printOrder() {
        return this;
    }

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getnRun() {
        return nRun;
    }

    public void setnRun(String nRun) {
        this.nRun = nRun;
    }

    public Row getHeader() {
        return header;
    }

    public void setHeader(Row header) {
        this.header = header;
    }

    public Row getParams() {
        return params;
    }

    public void setParams(Row params) {
        this.params = params;
    }

    public Results getResults() {
        return results;
    }

    public void setResults(Results results) {
        this.results = results;
    }

    public int getResultCount() {
        if (results==null) {
            return 0;
        }
        return results.size();
    }
    
    public class Row extends SimpleJsonList<String> {
        
        public String createKey() {
            StringBuilder sb = new StringBuilder();
            boolean first=true;
            for (int k=0; k<resultKeys.length; k++) {
                Integer column=resultKeys[k]-1;  // column numbers start with 1
                String value=rds[column].canonicalString(get(column));
                if (first) {
                    first=false;
                } else {
                    sb.append(',');
                }
                sb.append(value);
            }
            return sb.toString();
        }

    }
   
    public class Results extends SimpleJsonList<Row> {
        @Override
        public JsonList<?> newJsonList() {
            return new Row();
        }
    }

    public void setRowDescr(NamedResultDescriptionLists resultDescs) {
        this.rds=resultDescs.getResultDescriptions();
        this.resultKeys=resultDescs.getResultKeys();
    }

    public ResultDescription[] getRowDescr() {
        return rds;
    }

    public Integer[] getResultKeys() {
        return resultKeys;
    }

    public void setResultKeys(Integer[] resultKeys) {
        this.resultKeys = resultKeys;
    }

    private HashMap<String, Row> resultMap;
    
    public HashMap<String, Row> getResultMap() {
        if (resultMap!=null) {
            return resultMap;
        }
       if (resultKeys==null) {
           return null;
       }
       resultMap=new HashMap<String, Row>();
       for (Row row: results) {
           String key=row.createKey();
           resultMap.put(key, row);
       }
       return resultMap;
    }

}

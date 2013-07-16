package com.openlinksw.bibm.qualification;

import com.openlinksw.util.json.JsonWalker;

public class QueryResultAssembler {
    ResultDescription[] rds;
    private JsonWalker stack;
    int columnNo;
        
    public QueryResultAssembler(ResultDescription[] rds, JsonWalker stack) {
        this.stack = stack;
        this.rds=rds;
    }

    private ResultDescription getRd() {
        ResultDescription rd;
        if (rds == null) {
            rd = ResultDescription.stringResult;
        } else {
            rd = rds[columnNo];
            if (rd == null) {
                rd = ResultDescription.stringResult; 
            }
        }
        return rd;
    }

    public void startHeader() {
        stack.startEntry("header");
        stack.startList();
    }
    
    public void addColumn(String colName) {
        stack.addElement(colName);
    }

    public void endHeader() {
        stack.endEntry("header");
    }

    protected void appendAttrs(String distinct, String ordered) {
        stack. addEntry("distinct", distinct);
        stack.addEntry("ordered", ordered);
    }

    /**
     * writes actual parameters
     */
    public void appendParams(String[] params) {
        String key = "params";
        stack.startEntry(key);
        stack.startList();
        for (String param: params) {
            stack.addElement(param); 
        }
        stack.endEntry(key);
    }

    public void startResults() {
        String key = "results";
        stack.startEntry(key);
        stack.startList();
    }

    public void endResults() {
        String key = "results";
        stack.endEntry(key);
    }

    /** starts row of result data.
     *  Succeeding data should be saved with  respect of their type.
     */
    public void startRow() {
        stack.startList();
        columnNo=0;
    }

    public void addSqlRowElement(Object el) {
        ResultDescription rd=getRd();
        stack.addElement(rd.fromUnquoted(el));
        columnNo++;
    }

    public void addSparclRowElement(String value) {
       ResultDescription rd=getRd();
        stack.addElement(rd.fromQuoted(value));
        columnNo++;
    }

    public void endRow() {
        stack.endElement();
    }

}

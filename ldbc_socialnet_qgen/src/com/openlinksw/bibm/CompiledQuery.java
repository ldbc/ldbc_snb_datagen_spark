package com.openlinksw.bibm;

import com.openlinksw.bibm.Exceptions.BadSetupException;

public class CompiledQuery {
    private Query query;
    private Object[] params; // actual
    private byte queryType;
	private String name;
	private int queryMixRun;
	private String processedQueryString;
    private String[] queryStringSequence;
	
    public CompiledQuery(String name, byte queryType, String processedQueryString) {
        this.processedQueryString = processedQueryString;
        this.name = name;
        this.queryType = queryType;
    }
    
	public CompiledQuery(Query query, Object[] params, int nrRun) {
        this(query.getName(), query.getQueryType(), query.getProcessedQueryString(params, Integer.toString(nrRun)));
        this.params = params;
        this.query = query;
        this.queryMixRun = nrRun;
        byte[] typeSeq = query.getQueryTypeSequence();
        if (typeSeq==null) {
            queryStringSequence=new String[]{processedQueryString};
        } else {
            queryStringSequence=processedQueryString.split(";");
            if (queryStringSequence.length!=typeSeq.length) {
                throw new BadSetupException("Error in query "+name+": "+typeSeq.length 
                        +" parts declared but "+queryStringSequence.length+" found");
            }
        }
    }

    public Query getQuery() {
        return query;
    }

    /* return string representation of params, to be printed
     * 
     */
    public String[] getStringParams() {
        FormalParameter[] formalParams = query.getFormalParameters();
        String[] res=new String[params.length];
        for (int k=0; k<params.length; k++) {
            FormalParameter fp= formalParams[k];
            Object param=params[k];
            res[k]=fp.toString(param);
        }
        return res;
    }

    public String getName() {
		return name;
	}

    public String getQueryString() {
        return query.getQueryString();
    }

    public String getProcessedQueryString() {
        return processedQueryString;
    }

	public byte getQueryType() {
		return queryType;
	}

	public int getNr() {
	    int  qnr = Integer.parseInt(name)-1;
		return qnr;
	}

	public int getRun() {
		return queryMixRun;
	}

    public byte[] getQueryTypeSequence() {
        return query.getQueryTypeSequence();
    }

    public String[] getQueryStringSequence() {
        return queryStringSequence;
    }

}

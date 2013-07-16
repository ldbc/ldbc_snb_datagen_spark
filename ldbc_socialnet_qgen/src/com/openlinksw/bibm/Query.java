package com.openlinksw.bibm;

import static com.openlinksw.util.FileUtil.file2string;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.bsbm.ProdTypeLevelRange;
import com.openlinksw.bibm.qualification.ResultDescriptionLists;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.Nameable;
import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.SimpleJsonList;

@IgnoreCase
public class Query extends ResultDescriptionLists  implements Nameable {
    static final String BSBM_PREFIX = "query";
    static final String BSBM_QUERY_SUFFIX = ".txt";
    static final String BSBM_DESC_SUFFIX = "desc.txt";
    static final String RDFH_DESC_SUFFIX = ".desc";

    // query type constants
    public static final byte SELECT_TYPE = 1;
    public static final byte DESCRIBE_TYPE = 2;
    public static final byte CONSTRUCT_TYPE = 3;
    public static final byte UPDATE_TYPE = 4;
    /** sql errors ignored. Useful to drop table which  */
    public static final byte TRY_TYPE = 5;
    public static final byte CALL_TYPE = 6;

    private String prefix =Query.BSBM_PREFIX;
    private String qSuffix=Query.BSBM_QUERY_SUFFIX;

    //===========info from the query description //
    private String name; // taken from the query description file name
    private byte queryType;
    private HashMap<String, Integer> parameterNames = new HashMap<String, Integer>();     // Mapping of Parameter names to their array position
    private FormalParameter[] formalParameters;
    private ProdTypeLevelRange productTypeParameter;

    //===========info from the query text //
    private String queryString;
    private byte[] queryTypeSequence;

    private AbstractParameterPool parameterPool;
    private QueryMix queryMix;

	public Query(String qName, AbstractParameterPool parameterPool, File queryDir, File descDir) throws IOException {
		this.parameterPool=parameterPool;
		this.name=qName;
		queryType = SELECT_TYPE;// default: Select query
        File queryFile = new File(queryDir, prefix+qName+qSuffix);
        queryString = file2string(queryFile);
        processParameters(qName, queryDir);
        if (formalParameters==null) {
            if (descDir!=null) {
                processParameters(qName, descDir);
            }
        }
        if (formalParameters==null) {
            formalParameters=new FormalParameter[0];
        }
	}

    private void processParameters(String qName, File queryDir) throws IOException {
        File queryDescFile = new File(queryDir, prefix+qName+RDFH_DESC_SUFFIX); // rdfh descriptions take precedence
        if (queryDescFile.exists()) {
            super.loadFrom(queryDescFile);
            return;
		}
        queryDescFile = new File(queryDir, prefix+qName+BSBM_DESC_SUFFIX);
        if (queryDescFile.exists()) {
            processBsbmParameters(queryDescFile);
		}
    }

		/*
	 * Sets the Query Parameter Types and their places in the query, namely the
	 * two arrays parameterFills and parameterTypes
	 */
	private void processBsbmParameters(File parameterDescriptionFile) {
        String parameterDescriptionString = file2string(parameterDescriptionFile);
		// parameterType Array
		ArrayList<FormalParameter> formalParametersList = new ArrayList<FormalParameter>();
		// StringTokenizer for the parameter description String
		StringTokenizer paramTokenizer = new StringTokenizer(parameterDescriptionString);

		Integer index = 0;// Array index for ParameterTypes

		// Read Query Description
		while (paramTokenizer.hasMoreTokens()) {
			String line = paramTokenizer.nextToken();
			// Skip uninteresting lines
			if (!line.contains("="))
				continue;

			int offset = line.indexOf("=");
			// Parameter name
			String parameter = line.substring(0, offset);

			offset++;
			// Parameter Type
			String paramType = line.substring(offset);

			// If special parameter querytype is given save query type for later use
			if (parameter.toLowerCase().equals("querytype")) {
				byte qType = getQueryType(paramType);
				if (qType == 0) {
					DoubleLogger.getErr().println("Invalid query type chosen."	+ " Using default: Select");
				} else {
					queryType = qType;
				}
			}	else { 
				FormalParameter fp;
				String[] addPI = null;
				if(paramType.contains("_")) {
					String[] paramSplit = paramType.split("_");
					paramType = paramSplit[0];
					addPI = new String[paramSplit.length-1];
				    for (int k = 0; k < addPI.length; k++) {
				        addPI[k]=paramSplit[k+1];
                    }
				}
				fp = parameterPool.createFormalParameter(paramType, addPI, null);
				if (parameter.toLowerCase().equals("producttype") && fp instanceof ProdTypeLevelRange) { // sigh...
					productTypeParameter=(ProdTypeLevelRange)fp;
				}
				formalParametersList.add(fp);					
				parameterNames.put(parameter, index++);
			}
		} // end while()

		formalParameters = formalParametersList.toArray(new FormalParameter[formalParametersList.size()]);
	}

    /**
     * called indirectly by parser
     */
    @Override
    public JsonList<?> newJsonList(String key) {
        if (key.equalsIgnoreCase("sequence")) {
            return new SimpleJsonList<String>();
        } else   if (key.equalsIgnoreCase("params")) {
            return new SimpleJsonList<String>();
        } else {
            return super.newJsonList(key);
        }
    }

    /**
     * called indirectly by parser
     * @param queryTypeS
     */
    public void setSequence(SimpleJsonList<String> sequence) {
        queryTypeSequence=new byte[sequence.size()];
        for (int k=0; k<queryTypeSequence.length; k++) {
            queryTypeSequence[k]=getQueryType(sequence.get(k));
        }
    }

    /**
     * called indirectly by parser
     * @param queryTypeS
     */
    @SuppressWarnings("unchecked")
    public void setParams(SimpleJsonList<JsonObject> paramDescriptions) {
        formalParameters = new FormalParameter[paramDescriptions.size()];
        int index=0;
        for (JsonObject descr: paramDescriptions) {
            String paramName=(String)descr.get("name");
            String paramClass=(String)descr.get("class");
            JsonList<String> range=(JsonList<String>)descr.get("range");
            String[] addPI;
            if (range==null) {
                addPI=new String[0];
            } else {
                int spSize = range.size();
                addPI = range.toArray(new String[spSize]);
            }
            String defaultValue=(String)descr.get("default");
            FormalParameter  fp = parameterPool.createFormalParameter(paramClass, addPI, defaultValue);
            if (paramName.equalsIgnoreCase("producttype") && fp instanceof ProdTypeLevelRange) { // sigh...
                productTypeParameter=(ProdTypeLevelRange)fp;
            }
            formalParameters[index]=fp;                   
            parameterNames.put(paramName, index);
            index++;
        }
    }

    /**
     * called indirectly by parser
     * @param queryTypeS
     */
    public void setQueryType(String queryTypeS) {
        if (queryTypeS==null) {
            return;
        }
        byte qType = getQueryType(queryTypeS);
        if (qType == 0) {
            DoubleLogger.getErr().println("Invalid query type chosen."  + " Using default: Select");
        } else {
            this.queryType = qType;
        }
    }

	/*
	 * get the byte type representation of this query type string
	 */
	private byte getQueryType(String stringType) {
		if (stringType.equalsIgnoreCase("select"))
			return SELECT_TYPE;
		else if (stringType.equalsIgnoreCase("describe"))
			return DESCRIBE_TYPE;
		else if (stringType.equalsIgnoreCase("construct"))
			return CONSTRUCT_TYPE;
        else if (stringType.equalsIgnoreCase("update"))
            return UPDATE_TYPE;
        else if (stringType.equalsIgnoreCase("try"))
            return TRY_TYPE; // for DROP TABLE: execute and ignore errors; result is not expected
        else if (stringType.equalsIgnoreCase("call"))
            return CALL_TYPE; // call PL/SQL procedure
		else
			return 0;
	}

	   /*
     * returns a String of the Query with query parameters filled in.
     */
	public String getProcessedQueryString(Object[] params, String streamNumber) {
        StringBuilder s = new StringBuilder();

        // since parameterChar can be used for other purposes,
        // only take into account proper parameter occurrences
        char parameterChar = parameterPool.parameterChar;
        int index0 = 0;
        int index2 = -1;
        for (;;) {
            int index1 = queryString.indexOf(parameterChar, index2 + 1);
            if (index1==-1) break;

            index2 = queryString.indexOf(parameterChar, index1 + 1);
            if (index2==-1) break;

            String parameterName = queryString.substring(index1 + 1, index2);
            Integer parameterFill = parameterNames.get(parameterName);
            Object parameterValue=null;
            if (parameterFill!=null) {
                parameterValue=params[parameterFill];
            } else if (parameterName.equals("STREAM_ID")) {
                parameterValue=streamNumber;
            }
            if (parameterValue==null) {
                // this was just  not a parameter
                index2--; // 
                continue;
            } else {
                s.append(queryString, index0, index1);
                s.append(parameterValue);
                index0=index2+1;
            }
        }
        s.append(queryString.substring(index0));
		return s.toString();
	}

	public FormalParameter[] getFormalParameters() {
		return formalParameters;
	}

	public String getName() {
		return name;
	}

    public void setName(String newName) {
        this.name=newName;
    }

    public byte getQueryType() {
		return queryType;
	}

	public QueryMix getQueryMix() {
		return queryMix;
	}

	public void setQueryMix(QueryMix queryMix) {
		this.queryMix = queryMix;
	}

	public String[] getRowNames() {
		throw new UnsupportedOperationException();
	}

	public ProdTypeLevelRange getProdTypeLevelRange() {
		return productTypeParameter;
	}

    public String getQueryString() {
        return queryString;
    }

    public byte[] getQueryTypeSequence() {
        return queryTypeSequence;
    }

}

package com.openlinksw.bibm.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.ArrayList;
import java.io.*;

import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.ConnectionParameters;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.Exceptions.RequestFailedException;
import com.openlinksw.util.DoubleLogger;

public class NetQuery {
    static String encoding = "UTF-8";
	private static final int MAX_ERR_LINES = 20;
	ConnectionParameters parameters;
	HttpURLConnection conn;
	double executionTimeInSeconds;
	String defaultGraph;
	String queryString;
	String queryName;
	String urlString;
	URL url;
	byte queryType;
    int timeout;
    
    public NetQuery(CompiledQuery query, ConnectionParameters parameters) {
        String queryString = query.getProcessedQueryString();
        byte queryType = query.getQueryType();
        queryName = query.getName();
		this.queryString = queryString;
		this.parameters=parameters;
        this.timeout=parameters.timeout.getValue();
        this.defaultGraph=parameters.defaultGraph.getValue();
		String serviceURL=parameters.getNextEndPoint(queryType);
		try {
			char delim=serviceURL.indexOf('?')==-1?'?':'&';
			if(queryType==Query.UPDATE_TYPE)
				urlString = serviceURL;
			else {
                urlString = serviceURL + delim + "query=" + URLEncoder.encode(queryString, encoding);
				delim = '&';
                if (defaultGraph!=null)
                    urlString +=  delim + "default-graph-uri=" + defaultGraph;
			}
			this.queryType=queryType;
	        url = new URL(urlString);
		} catch(UnsupportedEncodingException e) {
     		throw new ExceptionException("Unsupported Encoding: "+encoding, e);
		} catch(MalformedURLException e) {
			throw new ExceptionException("bad URL: " + urlString, e);
		}
	}

    private void configureConnection() throws ProtocolException, IOException{
		conn = (HttpURLConnection)url.openConnection();

		if(queryType==Query.UPDATE_TYPE)
			conn.setRequestMethod("POST");
		else
			conn.setRequestMethod("GET");
		conn.setDefaultUseCaches(false);
		conn.setDoOutput(true);
		conn.setUseCaches(false);
        conn.setReadTimeout(timeout);
		if(queryType==Query.DESCRIBE_TYPE || queryType==Query.CONSTRUCT_TYPE)
//          conn.setRequestProperty("Accept", "application/rdf+xml");
            conn.setRequestProperty("Accept", "application/rdf+json");
		else
//          conn.setRequestProperty("Accept", "application/sparql-results+xml");
            conn.setRequestProperty("Accept", "application/sparql-results+json");
		
		if(queryType==Query.UPDATE_TYPE) {
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			OutputStream out = conn.getOutputStream();
			String queryParamName = parameters.sparqlUpdateQueryParameter.getValue() + "="; 
			out.write(queryParamName.getBytes());
			out.write(URLEncoder.encode(queryString, "UTF-8").getBytes());
			if (defaultGraph!=null) {
				out.write("&default-graph-uri=".getBytes());
				out.write(defaultGraph.getBytes());
			}
			out.flush();
		}
	}
	
    public InputStream exec() throws InterruptedException, IOException {
		long start = System.currentTimeMillis();
		long timeout = parameters.getRetryInt();
		String[] httpReplyBody=null;
		try {
		    if (parameters.isDryMode()) {
		        return null;
		    }
			int rc=0;
			int k=0;
			for ( ; ; ) {
                configureConnection();
				httpReplyBody=null;
				start = System.currentTimeMillis();
				try {
					conn.connect();
	                rc = conn.getResponseCode();
				} catch(IOException e) {
					throw new ExceptionException("Could not connect to SPARQL Service: "+conn.getURL(), e);
				}
				if (rc !=500) 	break;
				if (k++ >= parameters.numRetries.getValue()) break;
				String retryErrorMessage=parameters.retryErrorMessage.getValue();
				if (retryErrorMessage==null) break;
				httpReplyBody=readLines(conn.getErrorStream());
				String specificMessage=httpReplyBody[0];
				if (!specificMessage.startsWith(retryErrorMessage)) {
//					ErrLogger.getLogger().println("! \""+specificMessage+"\".startsWith("+parameters.recoverableError500Message);
					break;
				}
				DoubleLogger.getErr().println("* Query execution: Received error code 500/deadloack from server; trying to reconnect ...",
					"    (error message=", specificMessage, " )\n");
				Thread.sleep(timeout);
				timeout=(long)(timeout*1.5);
			}
			if (rc < 200 || rc >= 300) {
				reportError(httpReplyBody, "Received error code "+rc+" "+conn.getResponseMessage());
				throw new RequestFailedException();
			}
			return conn.getInputStream();
		} catch (SocketTimeoutException e) {
			return null;
        } finally {
            long interval = System.currentTimeMillis()-start;
            executionTimeInSeconds=interval/1000d;
		}

	}

    private void reportError(String[] httpReplyBody, String message) {
        DoubleLogger logger = DoubleLogger.getErr();
        logger.println("* Query execution: ", message); 
        logger.println("** For query: ", queryName, "\n");
        logger.println(urlString);
        if (httpReplyBody==null) {
        	InputStream errorStream = conn.getErrorStream();
            if (errorStream!=null) {
                httpReplyBody=readLines(errorStream);                   
            }
        }
        if (httpReplyBody==null) {
            logger.println("** No reply body.");
        } else {
            logger.println("** Reply body: ");
            for (String line: httpReplyBody) {
                logger.println(line);
            }
            logger.println("** (end Reply body)");
        }
    }
	
	private String[] readLines(InputStream is) {
		ArrayList<String> lines=new ArrayList<String>();
		BufferedReader br=new BufferedReader(new InputStreamReader(is));
		read:
		try {
			for (int k=0; k<MAX_ERR_LINES; k++) {
				String line = br.readLine();
				if (line == null) break read;
				lines.add(line);
			}
			lines.add("** (the rest of error message skipped)");
		} catch(IOException e) {
			lines.add("Could not read HTTP reply: "+e.getMessage());
		}
		if (lines.size()==0) {
			return new String[]{""};
		} else {
		    return lines.toArray(new String[lines.size()]);
		}
	}
	
    protected double getExecutionTimeInSeconds() {
		return executionTimeInSeconds;
	}
	
	protected void close() {
		conn.disconnect();
		conn = null;
	}
}

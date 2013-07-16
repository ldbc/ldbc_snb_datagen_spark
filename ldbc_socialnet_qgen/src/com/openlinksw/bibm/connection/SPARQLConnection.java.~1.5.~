package com.openlinksw.bibm.connection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.AbstractTestDriver;
import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.qualification.SparqlResult;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.json.DefaultJsonWalker;
import com.openlinksw.util.json.JsonParser;

public class SPARQLConnection implements ServerConnection{
    AbstractTestDriver driver;
    String queryName;
	
	public SPARQLConnection(AbstractTestDriver driver) {
		this.driver=driver;
	}
	
	public AbstractQueryResult executeQuery(CompiledQuery query) throws InterruptedException {
		byte queryType = query.getQueryType();
		queryName = query.getName();
		
		NetQuery 	qe = new NetQuery(query,  driver);
        try {
            InputStream is = qe.exec();
            SparqlResult  result = new SparqlResult(query, is);
            double timeInSeconds = qe.getExecutionTimeInSeconds();
            result.setCurrent(timeInSeconds);

            if (is==null) { // Timeout
                DoubleLogger.getOut().printf("Query %s: %d seconds timeout!\n", queryName, timeInSeconds);
                result.reportTimeOut();
                result.setCurrent(timeInSeconds);
                qe.close();
                return result;
            }
            
            
            if (queryType!=Query.SELECT_TYPE) {
                int resultCount =  countBytes(is);
                // TODO pass resultCount to statistic collector
            }            

            qe.close();
            return result;
        } catch (IOException e) {
            throw new ExceptionException("IOException: ", e);
        }
	}
	

private int countBytes(InputStream is) {
	int nrBytes=0;
	byte[] buf = new byte[10000];
	int len=0;
//	StringBuffer sb = new StringBuffer(1000);
	try {
		while((len=is.read(buf))!=-1) {
			nrBytes += len;//resultCount counts the returned bytes
//			String temp = new String(buf,0,len);
//			temp = "\n\n" + temp + "\n\n";
//			logger.log(Level.ALL, temp);
//			sb.append(temp);
		}
	} catch(IOException e) {
		DoubleLogger.getErr().println("Could not read result from input stream: "+e.toString());
	}
//	System.out.println(sb.toString());
	return nrBytes;
}
	
private int countResults(InputStream is)  {
    InputStreamReader ir=new InputStreamReader(is);
    BufferedReader br=new BufferedReader(ir);
    JsonParser parser;
    try {
        parser = new JsonParser(br, "answer from query "+queryName);
        Counter counter = new Counter();
        parser.parseTo(counter);
        return counter.counter;
    } catch (IOException e) {
        DoubleLogger.getErr().println("SAX Error").printStackTrace(e);
        return -1;
    }
}

class Counter extends DefaultJsonWalker {
    boolean bindingsSeen=false;
    int counter=0;


    @Override
    public void startEntry(String key) {
        if (key.equals("\"bindings\"")) {
            bindingsSeen=true;
        }
    }

    @Override
    public void endElement() {
        if (bindingsSeen) {
            counter++;
        }
    }

    @Override
    public void endEntry(String key) {
        if (key.equals("\"bindings\"")) {
            bindingsSeen=false;
        }
    }
}

private int countResultsXML(InputStream s)  {
    ResultHandler handler = new ResultHandler();
    int count=0;
    try {
      SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
//    ByteArrayInputStream bis = new ByteArrayInputStream(s.getBytes("UTF-8"));
      saxParser.parse( s, handler );
      count = handler.count;
    }  catch (Exception e) {
        DoubleLogger.getErr().println("SAX Error").printStackTrace(e);
        return -1;
    }
    return count;
}

	private static class ResultHandler extends DefaultHandler {
		int count=0;
		
		@Override
        public void startElement( String namespaceURI,
                String localName,   // local name
                String qName,       // qualified name
                Attributes attrs )
		{
			if(qName.equals("result"))
				count++;
		}
	}
	
	public void close() {
		//nothing to close
	}
	
 }

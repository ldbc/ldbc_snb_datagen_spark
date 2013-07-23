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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.Query;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.json.DefaultJsonWalker;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.JsonParser;
import com.openlinksw.util.json.ParserException;

public class SparqlResult extends AbstractQueryResult {
	
	public SparqlResult(CompiledQuery query, InputStream is) throws IOException {
		super(query);
        if (is==null) {
            return;
        }
        switch (query.getQueryType()) {
        case Query.SELECT_TYPE:
            assembleQueryResultfromJson(is);
            break;
        }
        is.close();                
	}

    /* Parsing pure JSON ============================*/
    public  void assembleQueryResultfromJson(InputStream is) throws IOException {
        QueryResultAssembler as=getQueryResultAssembler();
        String[] params=getQuery().getStringParams();
        
        InputStreamReader ir=new InputStreamReader(is);
        BufferedReader br=new BufferedReader(ir);
        JsonParser parser;
        try {
            parser = new JsonParser(br, "answer from query "+getQuery().getQuery().getName());
            DeQuoter DeQuoter = new DeQuoter(as, params);
            parser.parseTo(DeQuoter);
        } catch (ParserException e) {
            DoubleLogger.getErr().println("Warning: could not parse answer from query ", getQuery().getName(), e);
        }
    }
    
    class DeQuoter extends DefaultJsonWalker {
        QueryResultAssembler as;
        String[] params;
        Stack<String> stack=new Stack<String>();
        String value;

        public DeQuoter(QueryResultAssembler as, String[] params) {
            this.as=as;
            this.params=params;
            stack.push("root");
        }

        @Override
        public void startEntry(String key) {
            if (stack.peek().equals("row")) {
                stack.push("column");
                return;
            }
            key=stripQuotes(key);
            stack.push(key);
            if (key.equals("head")) {
                as.startHeader();
            } else  if (key.equals("results")) {
                as.appendParams(params);
                as.startResults();
            }
        }

        @Override
        public void pushValue(String value) {
            this.value=value;
        }

        @Override
        public JsonObject startObject() {
            if (stack.peek().equals("bindings")) {
                stack.push("row");
                as.startRow();
            }
            return null;
        }

        @Override
        public void endElement() {
            String top = stack.peek();
            if (top.equals("vars")) {
                as.addColumn(value);
            } else if (top.equals("row")) {
                stack.pop();
                as.endRow();
            }
        }

        @Override
        public void endEntry(String key) {
            key=stripQuotes(key);
            String savedKey = stack.pop();
            if (!savedKey.equals("column")) {
                if (!key.equals(savedKey)) {
                    throw new RuntimeException("endEntry("+key+") vs. \""+savedKey+'"');
                }
            }
            if (key.equals("head")) {
                as.endHeader();
            } else  if (key.equals("value")) {
                as.addSparclRowElement(value);
            } else  if (key.equals("results")) {
                as.endResults();
            }
        }

        private String stripQuotes(String key) {
        	if (key.startsWith("\"") && key.endsWith("\""))
              return key.substring(1, key.length()-1);
        	else
        	  return key;
        }

    }

    /* Parsing XML ============================*/
    
    protected  void assembleQueryResultfromXML(InputStream is) {
        QueryResultAssembler as=getQueryResultAssembler();
        String[] params=getQuery().getStringParams();
        try {
          SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
          XmlHandler handler=new XmlHandler(as, params);
          saxParser.parse(is, handler );
        }  catch (Exception e) {
            DoubleLogger.getErr().println("SAX Error").printStackTrace(e);
        }
    }

    class XmlHandler extends DefaultHandler {
        String[] params;
        QueryResultAssembler as;
        String value;
        
        public XmlHandler(QueryResultAssembler as, String[] params) {
            this.as=as;
            this.params=params;
            this.as = as;
        }

        @Override
        public void startElement( String namespaceURI,
                String localName,   // local name
                String qName,       // qualified name
                Attributes attrs ) {
            if (qName.equals("head")) {
                as.startHeader();
            } else  if (qName.equals("variable")) {
                as.addColumn(attrs.getValue("name"));
            } else  if (qName.equals("results")) {
                /* sql connection does not provide such attrs, anyway
                String distinct = attrs.getValue("distinct");
                String ordered = attrs.getValue("ordered");
                appendAttrs(distinct, ordered);
                */
                as.appendParams(params);
                as.startResults();
            } else  if (qName.equals("result")) {
                as.startRow();
            }
        }

        @Override
        public void characters(char[] ch, int start, int length)    throws SAXException {
            value=new String(ch, start, length);
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            if (qName.equals("head")) {
                as.endHeader();
            } else  if (qName.equals("results")) {
                as.endResults();
            } else  if (qName.equals("result")) {
                as.endRow();
            } else  if (qName.equals("literal")) {
                as.addSparclRowElement(value);
            }
        }        
    }

}

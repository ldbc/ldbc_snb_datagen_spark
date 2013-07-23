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
package com.openlinksw.util.json;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.openlinksw.bibm.Exceptions.ExceptionException;

/** at least one of following methods should be overriden:
 * newRootObject(),
 * newRootList()
 * 
 * @author ak
 *
 */
public class JsonParser  {//extends JsonStack {
	static final char LBRACE='{', RBRACE='}', LSQUARE='[', RSQUARE=']', COMMA=',', COLON=':'
		, SPACE=' ', TAB='\t', NEWL='\n', QUOTE='"', QUOTE2='\'', COMMENT='#'
		, ALPHA='\u03b1', EOF=Character.MAX_VALUE;
	private String filename;
	private BufferedReader in;
	private JsonWalker walker; 

    private String line;
	private int lineNumber=0;
	private int pos=0;
	private char cch;
	
	private String tokenLine;
	private int tokenLineNumber;
	private int tokenPos;
	private char tokenType;
	private String tokenValue;
	
    public JsonParser(BufferedReader in, String filename) throws IOException {
        this.filename=filename;
        this.in=in;
        for (;;) {
            line = in.readLine();
            lineNumber++;
            if (line == null) {
                throw new ParserException("json file is empty");
            }
            if (line.length()>0) {
                break;
            }
        }
        pos=0;
        cch=line.charAt(pos);
        scan();
    }

    public JsonParser(InputStreamReader ir, String filename) throws IOException {
        this(new BufferedReader(ir),filename);
    }

    public JsonParser(InputStream is, String filename) throws IOException {
        this(new InputStreamReader(is), filename);
    }

    public JsonParser(File inputFile) throws IOException {
        this(new BufferedReader(new FileReader(inputFile)), inputFile.getCanonicalPath());
    }

    private static String unquote (String val)
    {
    	if( val.startsWith("\"") && val.endsWith("\"") )
    		return val.substring(1, val.length()-1);
    	else
    	    return val;
    }
    public void parseTo(JsonWalker walker) throws IOException, ParserException {
        this.walker=walker;
        try {
            switch (tokenType) {
            case LSQUARE:
                parseList();
                break;
            case LBRACE:
                parseObject();
                break;
            default:
                throw new ParserException("{ or [ expected");
            }
        } catch (Exception e) {
            reThrowException(e);
        }
        walker.end();
    }

    protected void reThrowException(Exception e) {
        String exceptionId;
        Throwable cause=null;
        if (e instanceof ParserException) {
            exceptionId= "Syntax error";
        } else  if (e instanceof ExceptionException) {
            cause = e.getCause();
            exceptionId=cause.getClass().getName();
        } else {
            cause = e;
            exceptionId=e.getClass().getName();
        }
        StringBuilder sb=new StringBuilder();
        sb.append("\n").append(exceptionId).append(" in file:").append(filename).append(" at line ").append(tokenLineNumber).append(":\n");
        sb.append(tokenLine).append("\n");
        for (int k=0; k<tokenPos; k++) {
            sb.append(' ');
        }
        sb.append("^ ").append(e.getMessage());
        ParserException ee = new ParserException(sb.toString(), cause);
        ee.setStackTrace(e.getStackTrace());
        throw ee;
    }

	/**
     * allows spare comma at the end, but not at the beginning
	 * @return JsonArray
	 * @throws IOException
	 */
    protected void parseList() throws IOException {
		checkScan(LSQUARE);
		walker.startList();
		for (;;) {
			if (tokenType==RSQUARE) {
				scan();
				return;
			}
			parseValue();
			walker.endElement();
			switch (tokenType) {
			case RSQUARE:
				scan();
				return;
			case COMMA:
				scan();
				break;
			default:
                throw new ParserException(""+ALPHA+", comma, or ] expected");
			}
		}
	}
	
    /**
     * allows spare comma at the end, but not at the beginning
     * @return JsonObject
     * @throws IOException
     */
	protected void parseObject() throws IOException {
		checkScan(LBRACE);
		walker.startObject();
		for (;;) {
            if (tokenType==RBRACE) {
                scan();
                return;
            }
            String name=unquote (tokenValue);
            checkScan(ALPHA);
            checkScan(COLON);
			walker.startEntry(name);
			parseValue();
			walker.endEntry(name);
			switch (tokenType) {
			case RBRACE:
				scan();
				return;
			case COMMA:
				scan();
				break;
			default:
			    throw new ParserException(""+ALPHA+", comma, or } expected");
			}
		}
	}
	
	/** 
	 * string, array, or object
	 * @return
	 * @throws IOException
	 */
	private void parseValue() throws IOException {
		switch (tokenType) {
		case LBRACE:
			parseObject();
			return;
		case LSQUARE:
			parseList();
			return;
		case ALPHA: {
		    walker.pushValue(unquote(tokenValue));
			scan();
			return;
		}
		default: 
            throw new ParserException(""+ALPHA+", {, or [ expected");
		}
	}
	
	/** scanner=======================*/
	
	private char checkScan(char ch) throws IOException {
		if (tokenType!=ch) {
		    throw new ParserException(""+ch +" expected.");
		}
		return scan();
	}
	
	/**
	 * 
	 * @return next character
	 * @throws IOException
	 */
	private char nextChar() throws IOException {
	    pos++;
        if (pos==line.length()) {
            return cch=NEWL;
        } else  if (pos>line.length()) {
            line=in.readLine();
            lineNumber++;
            pos=0;
            if (line==null) {
                return cch=EOF;
            } else  if (line.length()==0) {
                return cch=NEWL;
            } 
        }
        return cch=line.charAt(pos);
	}
	
	/**
	 * determines current lexical token
	 * @return token type, equal to the start character, or alpha for literal
	 * @throws IOException
	 */
	private char scan() throws IOException {
		for (;;) {
 			switch (cch) {
			case EOF:
				tokenValue=null;
				return	tokenType=cch;
			case LBRACE: case RBRACE: case LSQUARE:
			case RSQUARE: case COMMA: case COLON:
		        tokenLineNumber=lineNumber;
 	            tokenLine=line;
				tokenPos=pos;
				tokenValue=null;
				tokenType=cch;
				nextChar();
				return tokenType;
			case SPACE: case TAB:	case NEWL:
				nextChar();
				continue;
			case COMMENT:
				pos=line.length()-1;
				nextChar();
				continue;
			case QUOTE:
			case QUOTE2:
				return scanQuoted(cch);
			default:
				return scanAlpha();
			} // end switch
		} //end for loop
	}

	/** parses quoted literal
	 * 
	 * @param quoteSymbol
	 * @throws IOException
	 */
	private char scanQuoted(char quoteSymbol) throws IOException {
	    tokenLineNumber=lineNumber;
	    tokenLine=line;
        tokenPos=pos;
		StringBuilder sb=new StringBuilder();
		scan_alpha:
		for (;;) {
			sb.append(cch);
			char nextChar = nextChar();
			if (nextChar==EOF) {
				break scan_alpha;
			} else if (nextChar==quoteSymbol) {
				sb.append(cch);
				nextChar();
				break scan_alpha;
			}
		} // end for loop
		tokenValue=sb.toString();
        return tokenType=ALPHA;
	}
	
	/**
	 * parses literal
	 * @throws IOException
	 */
	private char scanAlpha() throws IOException {
        tokenLineNumber=lineNumber;
        tokenLine=line;
        tokenPos=pos;
		StringBuilder sb=new StringBuilder();
		scan_alpha:
		for (;;) {
			sb.append(cch);
			switch (nextChar()) {
			case EOF:
			case LBRACE: case RBRACE: case LSQUARE:
			case RSQUARE: case COMMA: case COLON:
			case SPACE: case TAB:	case NEWL:
			case COMMENT:
				break scan_alpha;
			}
		} // end for loop
		tokenValue=sb.toString();
        return tokenType=ALPHA;
	}
	
}


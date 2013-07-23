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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.math.BigDecimal;

/** do not use append directly, use print/println
 * 
 */
public abstract class Printer extends JsonWalker {

	protected ArrayDeque<String> stack=new ArrayDeque<String>(); 
    private int indent=0;
    boolean nl=true;
    boolean quoteStrings=true;
    private ArrayList<String> values; 
    String value=null;
    String key=null;
        
    public Printer() {
    }

    protected abstract Printer append(char c);
    protected abstract Printer append(String s);
    
    protected String quote( String string )
    {
    	if (quoteStrings)
    	{
    		if( string.startsWith("\"") && string.endsWith("\"") )
    			return string;
    		else if( string.startsWith("\'") && string.endsWith("\'") )
    			return "\"" + string.substring(1, string.length()-1) + "\"";
    		else
    			return "\"" + string + "\"";
    	}
    	else
    		return string;
    }
    
    protected static boolean isNumber(String val) {
    	try
    	{
    		BigDecimal nval = new java.math.BigDecimal( val );
    		return true;
    	}
    	catch (NumberFormatException e)
    	{
    		return false;
    	}
    }

    protected void decIndent() {
       indent--;
    }

    protected void incIndent() {
        indent++;
    }

    protected Printer print(String el) {
        if (nl) {
            for (int k=0; k<indent; k++) {
                append(' ');
            }
            nl=false;
        }
        append(el);
        return this;
    }

    protected Printer println(String el) {
        print(el);
        return println();
    }

    protected Printer println() {
        append('\n');
        nl=true;
        return this;
    }

    public void comment(String comm) {
    	// real JSON does not allow comments.
        // print("#").println(comm);
    }
    
    private void startComplex() {
        if (values!=null) {
            incIndent();
            boolean first=true;
            for (String value: values) {
                if (first) {
                    first=false;
                } else {
                    println(",");  
                }
                print(value);
            }
            if (!first) {
                println(",");
            } else {
                println();
            }
        }
        if (key!=null) {
            print(quote(key)).print(":");
            key=null;
        }
        values=new ArrayList<String>();
    }

    private void flushValues() {
        if (values!=null) {
            // closing simple list or object
            boolean first=true;
            for (String value: values) {
                if (first) {
                    first=false;
                } else {
                    print(", ");  
                }
                print(value);
            }
            values=null;
        } else {
            decIndent();
        }
    }
    
    @Override
    public JsonList<?> startList() {
        startComplex();
        print("[");
        stack.push("]");
        return null;
    }

    /** this may be end of an object, a list, or a simple element
     * 
     */
    @Override
    public void endElement() {
        if (value==null) {
            // end of complex
            flushValues();
            print(stack.pop());
            println(",");
        } else {
            // end of simple
            if (values==null) {
                print(value).println(",");
            } else {
                values.add(value);
            }
            value=null;
        }
    }

    @Override
    public void startEntry(String key) {
        this.key=key;
    }

    @Override
    public void endEntry(String key) {
        if (value!=null) 
        { // end simple
            if (values==null) {
                print(quote(key)).print(":").print(value).println(",");
            } else {
                values.add(quote(key)+":"+value);
            }
            this.key=null;
            value=null;
        }
        else
        {   // end complex
	        flushValues();
	        print(stack.pop());
	        println(",");
        }
    }

    @Override
    public void pushValue(String value) {
    	if (isNumber(value))
            this.value=value;
    	else
    		this.value=quote(value);
    }

    @Override
    public JsonObject startObject() {
        startComplex();
        print("{");
        stack.push("}");
        return null;
    }

    @Override
    public void end() {
        flushValues();
        print(stack.pop());
        println();
    }

}

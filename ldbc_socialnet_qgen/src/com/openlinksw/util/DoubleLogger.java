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
package com.openlinksw.util;

import java.io.*;
import java.util.Formatter;
import java.util.Locale;

/**
 * prints  both into the System.err and the log file, if any.
 * @author ak
 *
 */
public class DoubleLogger implements Appendable {
	private PrintStream out = System.out;
    private PrintStream out2;
    private Formatter formatter;

    public DoubleLogger(PrintStream out)   {
    	this.out=out;
        formatter=new Formatter(this, Locale.US);
    }

    public DoubleLogger(PrintStream out, File file) throws IOException   {
    	this(out);
        out2 = new PrintStream(new FileOutputStream(file));
    }


    /**
     * @throws IOException 
     */
    public static void setOutFile(File file) throws IOException   {
        thrLocalErr.set(new DoubleLogger(System.out, file));
    }

    public synchronized DoubleLogger print(char c)  {
        out.print(c);
        if (out2==null) return this;
        out2.print(c);
        return this;
    }

    public synchronized DoubleLogger print(Object... text)   {
        for(int k = 0; k < text.length; k++)    {
            Object str = text[k];
    		out.print(str);
    		if (out2!=null)	out2.print(str);
        }
        return this;
    }

    public synchronized DoubleLogger println(Object... text)   {
        for(int k = 0; k < text.length; k++)    {
            Object str = text[k];
    		out.print(str);
    		if (out2!=null) out2.print(str);
        }
		out.println();
		if (out2==null) return this;
		out2.println();
        return this;
    }

    public void printStackTrace(Throwable e) {
    	e.printStackTrace(out);
		if (out2==null) return;
    	e.printStackTrace(out2);
    }
    
    public void flush() {
        out.flush();
        if (out2==null) return;
        out2.flush();
        return;
    }

    @Override
    public Appendable append(CharSequence csq) throws IOException {
        out.append(csq);
        if (out2==null) return this;
        out2.append(csq);
        return this;
    }

    @Override
    public Appendable append(char c) throws IOException {
        out.append(c);
        if (out2==null) return this;
        out2.append(c);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException {
        out.append(csq, start, end);
        if (out2==null) return this;
        out2.append(csq, start, end);
        return this;
    }

    public DoubleLogger printf(String format, Object... txt) {
        formatter.format(format, txt);
        return this;
    }

    private static InheritableThreadLocal<DoubleLogger> thrLocalErr = new InheritableThreadLocal<DoubleLogger>() {

        public DoubleLogger initialValue()  {
            return new DoubleLogger(System.err);
        }

    };

    public static DoubleLogger getErr()    {
        return thrLocalErr.get();
    }

    /**
     * @throws IOException 
     */
    public static void setErrFile(File file) throws IOException   {
    	thrLocalErr.set(new DoubleLogger(System.err, file));
    }

    private static InheritableThreadLocal<DoubleLogger> thrLocalOut = new InheritableThreadLocal<DoubleLogger>() {

        public DoubleLogger initialValue()  {
            return new DoubleLogger(System.out);
        }

    };

    public static DoubleLogger getOut()    {
        return thrLocalOut.get();
    }

}

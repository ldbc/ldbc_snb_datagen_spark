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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Exec {
    
    public static String[] execProcess(String[] args) throws IOException, InterruptedException {
        if (args==null || args.length==0) {
            throw new IllegalArgumentException("empty exec arguments");
        }
        ProcessBuilder pb=new ProcessBuilder(args);
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode == 0) {
            String[] res= readOut(process.getInputStream());
        	return res;
        } else {
        	StringBuilder sb=new StringBuilder();
        	sb.append("Error "+exitCode+" when executing '");
        	for (int k=0; k<args.length; k++) {
        		String arg=args[k];
        		sb.append(arg);
//        		if (k<(args.length-1)) {
        			sb.append(' ');
//        		}
        	}
        	sb.append("': ");
        	String[] lines = readOut(process.getErrorStream());
            for (int i = 0; i < lines.length; i++) {
                sb.append(lines[i]).append('\n');
            }
            System.err.println(sb.toString());
        	return null;
        }
    }

    protected static  String[] readOut(InputStream is) {
        InputStreamReader isr=new InputStreamReader(is);
        BufferedReader br=new BufferedReader(isr);
        ArrayList<String> res=new ArrayList<String>(); 
        try {
            for (;;) {
            	String line=br.readLine();
            	if (line==null) break;
                res.add(line);
            }
        } catch (Exception e) {
            String msg = "Got exception while reading output stream ";
            System.err.println(msg + e);
            e.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                String msg = "Got exception while closing exhausted output stream ";
                System.err.println(msg + e);
                e.printStackTrace();
            }
        }
        return res.toArray(new String[res.size()]);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
		String[] res=execProcess(args[0].split("\\s+"));
		for (String line: res) {
			System.out.println(line);
		}
	}

}

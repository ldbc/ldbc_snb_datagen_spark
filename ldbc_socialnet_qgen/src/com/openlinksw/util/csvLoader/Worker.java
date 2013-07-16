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
package com.openlinksw.util.csvLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import com.csvreader.CsvReader;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.csv2ttl.CsvField;
import com.openlinksw.util.csv2ttl.CsvTableSchema;
import com.openlinksw.util.json.JsonList;

public class Worker implements Runnable {
	static final int MAX_FILE_BUFFER_SIZE = 64*1024;

    DoubleLogger err = DoubleLogger.getErr();
    DoubleLogger out = DoubleLogger.getOut();
	CsvLoader csvLoader;
	protected Connection conn;
	HashMap<String, Integer> paramCounts=new HashMap<String, Integer>(); 
	HashMap<String, PreparedStatement> statements=new HashMap<String, PreparedStatement>(); 
	
	public Worker(CsvLoader csvLoader) {
		this.csvLoader=csvLoader;
		try {
			conn = DriverManager.getConnection(csvLoader.endPoint, csvLoader.connectionProperties);
		} catch (SQLException e0) {
			SQLException e=e0;
			while(e!=null) {
				e.printStackTrace();
				e=e.getNextException();
			}
			throw new ExceptionException("SQLConnection()", e0);
		}
	}

    private int getParamCount(String tableName) {
        CsvTableSchema tableSchema=csvLoader.dbSchema.getTable(tableName);
        JsonList<CsvField> fields = tableSchema.getFields();
        return fields.size();
    }


	private PreparedStatement createPreparedStatement(CsvTableSchema tableSchema) throws SQLException {
         String updateString=tableSchema.createUpdateString();
         PreparedStatement res=null;
        try {
            res = conn.prepareStatement(updateString);
            return res;
        } catch (SQLException e) {
//            err.println(e.toString());
            err.println(updateString);
            throw e;
        }
    }

	void loadFile(File src) throws IOException, SQLException {
	    out.println("start loading file ", src, "...");
        String fileName=src.getName();
		String[] parts=fileName.split("\\.");
		if (parts.length<2 || !parts[1].equals(csvLoader.ext)) {
			// not an error, probably we are passed something like"data/*" 
			// err.println("Source file name, skipped: "+destFileName);				
		    out.println("  ... file ", src, " is not a .", csvLoader.ext);
			return;
		}
		String tableName=parts[0];
        CsvTableSchema tableSchema=csvLoader.dbSchema.getTable(tableName);
        if (tableSchema==null) {
            out.println("no schema for :", tableName);
            return;
        }
		BufferedReader freader = new BufferedReader(new FileReader(src)	, MAX_FILE_BUFFER_SIZE);
		CsvReader reader=new CsvReader(freader, '|');
		PreparedStatement stmt=createPreparedStatement(tableSchema) ;
        int paramCount=getParamCount(tableName);
        int batchSize=2000;
        int recN=0;
        boolean eof=false;
        for ( ; ; ) {
            int n;
            String[] values=null;
            for (n=0; n < batchSize; n++) {
                if (!reader.readRecord()) {
                    eof=true;
                    break;
                }
                recN++;
                values = reader.getValues();
                if (values.length < paramCount) {
                    throw new BadSetupException("in file:" + src + ", record:" + recN + " has " + values.length + " columns; " + paramCount + " requred");
                }
                tableSchema.setValues(stmt, values);
                stmt.addBatch();
            }
            if (n==0) {
                break;
            }
            try {
                stmt.executeBatch();
 //               out.println("  executeUpdate passed for "+tableName+" rec:"+recN);
            } catch (SQLException e) {
                err.println("  executeUpdate failed for: ", src.getAbsolutePath(), "; rec=", recN);
                err.print("values=[");
                for (int k = 0; k < paramCount; k++) {
                    err.print(values[k]);
                    err.print(" ,");
                }
                err.println("]");
                throw e;
            }
            if (eof) {
                break;
            }
		}
        stmt.close();
        out.println("  ... file ", src+": "+recN+" loaded");
	}

    @Override
	public void run() {
		for (int k=0; ; k++) {
			File f = csvLoader.getNextTask();
			if (f==null) {
			    out.println("Worker exiting after "+k+" loadings.");
			    return;
			}
			try {
				loadFile(f);
			} catch (Exception e) {
				if (csvLoader.reportFailure(f.getAbsolutePath(), e)) {
					return;
				}
			}
		}
	}

}

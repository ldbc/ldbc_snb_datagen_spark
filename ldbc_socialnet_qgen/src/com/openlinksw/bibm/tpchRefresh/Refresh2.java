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
package com.openlinksw.bibm.tpchRefresh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.csvreader.CsvReader;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.Exceptions.RequestFailedException;
import com.openlinksw.util.DoubleLogger;

public class Refresh2 extends AbstractTestDriver {
    File deleteFile;
    private Connection conn;
    
    public Refresh2(String args[]) throws IOException {
        super(version, "Usage: com.openlinksw.bibm.tpchRefresh.Refresh2 <options> endpoints...", // CHECK package
            "endpoint: The URL of the HTTP SPARQL or SQL endpoint");
        processProgramParameters(args);
        deleteFile=new File(updateDir, "delete."+dsN.getValue());
        if (!deleteFile.exists()) {
            fatal("File not exisis:"+deleteFile.getCanonicalPath());
        }
        System.out.println("..done");
        try {
            conn = DriverManager.getConnection(endPoint);
        } catch (SQLException e0) {
            SQLException e=e0;
            while(e!=null) {
                e.printStackTrace();
                e=e.getNextException();
            }
            throw new ExceptionException("SQLConnection()", e0);
        }
        
    }

    private PreparedStatement createPreparedStatement(String updateString) throws SQLException {
       PreparedStatement res=null;
       try {
           res = conn.prepareStatement(updateString);
           return res;
       } catch (SQLException e) {
//           System.err.println(e.toString());
           System.err.println(updateString);
           throw e;
       }
   }

    public void run() throws Exception {
        BufferedReader freader = new BufferedReader(new FileReader(deleteFile) , 0x10000);
        CsvReader reader=new CsvReader(freader, '|');
        PreparedStatement stmt1=createPreparedStatement("DELETE FROM ORDERS WHERE O_ORDERKEY =?") ;
        PreparedStatement stmt2=createPreparedStatement("DELETE FROM LINEITEM WHERE L_ORDERKEY  =?") ;
        int batchSize=2000;
        int recN=0;
        boolean eof=false;
        for ( ; ; ) {
            int n;
            String[] values=null;
            for (n=0; ; ) {
                if (!reader.readRecord()) {
                    eof=true;
                    break;
                }
                values = reader.getValues();
                for (int k = 0; k < values.length; k++) {
                    String value = values[k];
                    if (value.length()==0) {
                        // dbgen emits empy fields at the end of records
                        continue; 
                    }
                    stmt1.setLong(k+1, Long.parseLong(value));
                    stmt1.addBatch();
                    recN++;
                    n++;
                }
                if (n>= batchSize) {
                    break;
                }
            }
            if (n==0) {
                break;
            }
            stmt1.executeBatch();
            stmt2.executeBatch();
            if (eof) {
                break;
            }
        }
        stmt1.close();
        stmt2.close();
        System.out.println("  ... file "+deleteFile.getCanonicalPath()+": "+recN+" processed");
        
    }

    public static void main(String argv[]) throws InterruptedException {
        Refresh2 testDriver = null;
        int res=1;
        try {
            testDriver = new Refresh2(argv);
            testDriver.run();
            res=0;
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(res);
        }
    }
}

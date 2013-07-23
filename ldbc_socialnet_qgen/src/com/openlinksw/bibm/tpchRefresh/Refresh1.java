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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
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
import com.openlinksw.util.csv2ttl.CsvTableSchema;
import com.openlinksw.util.csv2ttl.DBSchema;

public class Refresh1 extends AbstractTestDriver {
    static int BUFF_LEN = 0x10000;
    ChainedCsvReader orders;
    ChainedCsvReader lineitems;
    private Connection conn;
    private DBSchema dbSchema;

    public Refresh1(String args[]) throws Exception {
        super(version, "Usage: com.openlinksw.bibm.tpchRefresh.Refresh1 <options> endpoints...", // CHECK package
                "endpoint: The URL of the HTTP SPARQL or SQL endpoint");
        StringOption schema=new StringOption("schema <conversion schema> (json file)", null);
        processProgramParameters(args);
        lineitems = new ChainedCsvReader("lineitem.tbl.u");
        orders = new ChainedCsvReader("orders.tbl.u");
        System.out.println("..done");
        try {
            conn = DriverManager.getConnection(endPoint);
        } catch (SQLException e0) {
            SQLException e = e0;
            while (e != null) {
                e.printStackTrace();
                e = e.getNextException();
            }
            throw new ExceptionException("SQLConnection()", e0);
        }

        String schemaName = schema.getValue();
        if (schemaName==null) {
            fatal("No schema provided. Exiting. ");
        }
        try {
            dbSchema=new DBSchema(schemaName);
        } catch (Exception e) {
            System.err.println("Problems loading schema file "+schemaName);
            System.err.println(e.getMessage());
            throw e;
        }

    }

    private PreparedStatement createPreparedStatement(String updateString) throws SQLException {
        PreparedStatement res = null;
        try {
            res = conn.prepareStatement(updateString);
            return res;
        } catch (SQLException e) {
            // System.err.println(e.toString());
            System.err.println(updateString);
            throw e;
        }
    }

    public void run() throws Exception {
        CsvTableSchema tableSchema1=dbSchema.getTable("orders");
        CsvTableSchema tableSchema2=dbSchema.getTable("lineitem");
        String updateString1=tableSchema1.createUpdateString();
        String updateString2=tableSchema2.createUpdateString();
        PreparedStatement stmt1 = createPreparedStatement(updateString1);
        PreparedStatement stmt2 = createPreparedStatement(updateString2);
        for (;;) {
            String[] values1 = orders.getValues();
            if (values1==null) {
                break;
            }
            tableSchema1.setValues(stmt1, values1);
            stmt1.execute();
            int o_orderKey=Integer.parseInt(values1[0]);
            
            for (;;) {
                String[] values2 = lineitems.getValues();
                if (values2==null) {
                    break;
                }
                int l_orderKey=Integer.parseInt(values2[0]);
                if (l_orderKey!=o_orderKey) {
                    lineitems.pushBack();
                    break;
                }
                tableSchema2.setValues(stmt2, values2);
                stmt2.addBatch();
            }
            stmt2.executeBatch();
        }
        stmt1.close();
        stmt2.close();
    }

    class ChainedCsvReader {
        String[] names;
        int indx = 0;
        File file ;
        int recN = 0;
        BufferedReader freader;
        CsvReader reader;
        String[] lastValues=null;
        String[] values=null;

        ChainedCsvReader(String start) throws IOException {
            this.names = lsUpdateDir(start);
            initReader();
        }

        String[] lsUpdateDir(String start) throws IOException {
            final String start2 = start + dsN.getValue();
            FilenameFilter filter = new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith(start2);
                }

            };
            String[] res = updateDir.list(filter);
            if (res.length == 0) {
                fatal("No " + start2 + "* files in " + updateDir.getCanonicalPath());
            }
            return res;
        }

       private void initReader() throws FileNotFoundException {
            file = new File(updateDir, names[indx]);
            freader = new BufferedReader(new FileReader(file), BUFF_LEN);
            reader = new CsvReader(freader, '|');
            lastValues=null;
            values=null;
        }

        private boolean readRecord() throws IOException {
            for (;;) {
                if (reader.readRecord()) {
                    return true;
                }
                System.out.println("  ... file "+file.getCanonicalPath()+": "+recN+" processed");
                if (indx == names.length - 1) {
                    return false;
                }
                indx++;
                initReader();
            }
        }

        public String[] getValues() throws IOException {
            if (values!=null) {
                lastValues=values;
                values=null;
            } else if (!readRecord()) {
                return null;
            } else {
                lastValues=reader.getValues();
                recN++;
            }
            return lastValues;
        }
        
        public void pushBack() {
            values=lastValues;
        }

    }

    public static void main(String argv[]) throws InterruptedException {
        Refresh1 testDriver = null;
        int res=1;
        try {
            testDriver = new Refresh1(argv);
            testDriver.run();
            res=0;
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(res);
        }
    }
}

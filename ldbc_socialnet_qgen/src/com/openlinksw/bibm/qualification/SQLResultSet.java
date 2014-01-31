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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;

public class SQLResultSet extends AbstractQueryResult {
	public String[] metadata;
	public ArrayList<Object[]> results=new ArrayList<Object[]>();
 	
    public SQLResultSet(CompiledQuery query) throws SQLException {
        super(query);
    }

    public SQLResultSet(CompiledQuery query, Statement statement) throws SQLException {
        super(query);
        for (;;) {
            ResultSet resultset = statement.getResultSet();
            if (resultset==null) {
                break;
            }
            addResultSet(resultset);
            if  (!statement.getMoreResults()) {
                break;
            }
        }
        assembleQueryResult();            
    }

    public  void assembleQueryResult() {
        QueryResultAssembler as=getQueryResultAssembler();
        String[] params=getQuery().getStringParams();
        as.startHeader();
        if (metadata!=null) {
            for (String colName: metadata) {
                as.addColumn("'"+colName+"'");
            }
        }
        as.endHeader();
        
        as.appendParams(params);
        
        as.startResults();
        for (Object[] row: results) {
            as.startRow();
            for (Object el: row) {
                as.addSqlRowElement(el);
            }
            as.endRow();
        }
        as.endResults();
    }

    public void addResultSet(ResultSet resultset) {
        try {
			ResultSetMetaData rsmd = resultset.getMetaData();
			int colCount = rsmd.getColumnCount();
			if (getRds() != null && colCount!=getRds().length) {
			    throw new BadSetupException("For query "+getqName()+": "+getRds().length+" result columns described but "+colCount+" got");
			}
			metadata=new String[colCount];
			for (int i = 0; i < colCount; i++) {
				metadata[i]=rsmd.getColumnName(i+1);
			}

			while (resultset.next()) {
				Object[] row=new Object[colCount];
	            for (int i = 0; i < colCount; i++) {
					row[i] = resultset.getObject(i+1);
				}
				results.add(row);
			}
		} catch (Exception e) {
			throw new ExceptionException("error transforming sql results to xml:", e);
		}
    }
	
    public String getXMLstring() {
        if (metadata==null) {
            return "";
        }
		StringBuilder sb=new StringBuilder();
        int colCount = metadata.length;
        sb.append("<resultset>\n");
        sb.append(" <head>\n");
        for (int i = 1; i <= colCount; i++) {
            String columnName = metadata[i];
            sb.append("   <variable name=\"").append(columnName).append("\">\n");
        }
        sb.append(" </head>\n");
        sb.append(" <results>\n");
        
        for (Object[] row: results) {
            sb.append("  <result>\n");
            for (int i = 1; i <= colCount; i++) {
                Object value = row[i];
                sb.append("   <literal>").append(value).append("</literal>\n");
            }
            sb.append("  </result>\n");
        }
        sb.append(" <results>\n");
        sb.append("<resultset>\n");

        return sb.toString();
	}
	
    @Override
    public int getResultCount() {
        return results.size();
    }

}

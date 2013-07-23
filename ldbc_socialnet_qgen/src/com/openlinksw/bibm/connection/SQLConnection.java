package com.openlinksw.bibm.connection;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.qualification.SQLResultSet;

public class SQLConnection implements ServerConnection {
    public static int fetchSize = 100;

    private int timeoutInSeconds;
    private Statement statement;
	protected Connection conn;

    public SQLConnection() {
    }

    public SQLConnection(String driverClassName, String endPoint, Properties info,	int timeoutInSeconds) {
	    this.timeoutInSeconds=timeoutInSeconds;
		try {
			Class.forName(driverClassName);
		} catch(ClassNotFoundException e) {
			throw new ExceptionException("Driver class not found:", e);
		}
		try {
		    if (info==null) {
		        conn = DriverManager.getConnection(endPoint);
		    } else {
                conn = DriverManager.getConnection(endPoint, info);
		    }
			statement = conn.createStatement();
			
			statement.setQueryTimeout(timeoutInSeconds);
			statement.setFetchSize(fetchSize);
		} catch (SQLException e0) {
			SQLException e=e0;
			while(e!=null) {
				e.printStackTrace();
				e=e.getNextException();
			}
			throw new ExceptionException("SQLConnection()", e0);
		}
	}
	
    public SQLConnection(String driverClassName, String endPoint,  int timeoutInSeconds) {
        this(driverClassName, endPoint, null, timeoutInSeconds);
    }
	/*
	 * Execute Query with precompiled Query
	 * @see benchmark.testdriver.ServerConnection#executeQuery(benchmark.testdriver.CompiledQuery, benchmark.testdriver.CompiledQueryMix)
	 */
	public AbstractQueryResult executeQuery(CompiledQuery query) {
		double timeInSeconds;
		String queryString=null;
        SQLResultSet result = null;         
		
		try {
			long start = System.currentTimeMillis();
			
			byte[] sequence = query.getQueryTypeSequence();
            if (conn==null) { // dry-run mode
                result = new SQLResultSet(query);
                result.reportTimeOut(); 
            } else  if (sequence==null) {
			    // simple query
			    queryString=query.getProcessedQueryString();
                result = runSimpleQuery(query, queryString, query.getQueryType());
			} else {
			    // complex query 
			    String[] parts=query.getQueryStringSequence();
			    for (int k=0; k<parts.length; k++) {
			        queryString=parts[k];
			        byte queryType = sequence[k];
			        SQLResultSet result2 = runSimpleQuery(query, queryString, queryType);
			        if (queryType==Query.SELECT_TYPE) {
			            result=result2;
			        }
			    }
                if (result==null) {
                    throw new BadSetupException("error in query "+query.getName()+": no part of type 'select' found.");
                }
			}
			
			long stop = System.currentTimeMillis();
			timeInSeconds = (stop-start)/1000d;
			result.setTimeInSeconds(timeInSeconds);
			return result;
		} catch(SQLException e0) {
			SQLException e=e0;
			while(e!=null) {
				e.printStackTrace();
				e=e.getNextException();
			}
			throw new ExceptionException("\n\nError for Query " + query.getName() + ":\n\n" + queryString, e0);
		}
	}

    private SQLResultSet runSimpleQuery(CompiledQuery query, String queryString, byte queryType) throws SQLException {
        SQLResultSet result=null;
        switch (queryType) {
        case Query.TRY_TYPE:
            try {
                statement.execute(queryString);
            } catch (SQLException e) {
            }
            break;
        case Query.UPDATE_TYPE:
            statement.executeUpdate(queryString);
            break;
        case Query.SELECT_TYPE: 
            statement.execute(queryString);
            result = new SQLResultSet(query, statement);
            break;
        case Query.CALL_TYPE: {
            CallableStatement cstmt=conn.prepareCall(queryString);
            cstmt.setQueryTimeout(timeoutInSeconds);
            cstmt.setFetchSize(fetchSize);
            cstmt.registerOutParameter(1, java.sql.Types.ARRAY); // FIXME
            cstmt.execute();
            result = new SQLResultSet(query, cstmt);
            break;}
         default:
             throw new BadSetupException("error in query "+query.getName()+": unsupported query type:"+queryType);
        }
        return result;
    }

	public void close() {
	    if (conn==null) {
	        return;
	    }
		try {
		     conn.close();
		} catch(SQLException e) {
			 throw new ExceptionException("SQLConnection.close()", e, true);
		}
	}

}

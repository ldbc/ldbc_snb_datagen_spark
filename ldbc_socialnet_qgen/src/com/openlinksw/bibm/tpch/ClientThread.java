package com.openlinksw.bibm.tpch;

import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.AbstractTestDriver;
import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.CompiledQueryMix;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.bibm.connection.SPARQLConnection;
import com.openlinksw.bibm.connection.SQLConnection;
import com.openlinksw.bibm.connection.ServerConnection;
import com.openlinksw.bibm.tpch.ExternalQuery;
import com.openlinksw.util.DoubleLogger;

public class ClientThread extends Thread {
    private ClientManager manager;
    CompiledQueryMix queryMix;
    int streamId;
    private ServerConnection conn;
	protected StreamStatistics streamStat;
	
	ClientThread(ClientManager parent, CompiledQueryMix qMix) {
		this.manager = parent;
		this.queryMix = qMix;
        streamId = queryMix.getRun();
		AbstractTestDriver driver=parent.driver;
		int timeoutInSeconds = driver.timeout.getValue()/1000;
        if (driver.isDryMode()) {
            conn = new SQLConnection();
        } else if (driver.doSQL.getValue()) {
	        String driverClassName = driver.driverClassName.getValue();
	        String endPoint = driver.getNextEndPoint(Query.SELECT_TYPE);
			conn = new SQLConnection(driverClassName, endPoint, parent.connectionProperties, timeoutInSeconds);
		} else {
			conn = new SPARQLConnection(driver);
	   }
        streamStat = new StreamStatistics(qMix.getRun());
	}
	
    private void _run() throws InterruptedException {
        long start = System.currentTimeMillis();
        streamStat.setqStart(start);
        Object[] queries = queryMix.getQueryMix();
        for (Object nextQ: queries) {
            if (nextQ instanceof CompiledQuery) {
                CompiledQuery next = (CompiledQuery) nextQ;
                AbstractQueryResult result = conn.executeQuery(next);
                String qName = next.getName();
                if (result.isTimeout()) {
                    streamStat.reportTimeOut(qName, result.getTimeInSeconds());
                } else  if ( result.getQueryMixRun() >= 0) {
                    streamStat.setCurrent(qName, result.getResultCount(), result.getTimeInSeconds());
                }
                manager.addResult(result);
            } else if (nextQ instanceof ExternalQuery) {
                ExternalQuery next = (ExternalQuery) nextQ;
                next.exec();
                streamStat.addRF(next);
            }
        }
        long end = System.currentTimeMillis();
        streamStat.setqEnd(end);
        DoubleLogger.getOut().printf("Stream %d:  %.3f s\n", streamId,  (end-start)/(double)1000);
        manager.addStreamStat(streamStat);
	}

	@Override
    public void run() {
		DoubleLogger logger = DoubleLogger.getErr();
		boolean success=false;
		try {
			_run();
			success=true;
		} catch (InterruptedException e) {
			logger.println("Client Thread ", streamId, " interrupted. Quitting...");
		} catch (ExceptionException e) {
			Throwable cause=e.getCause();
			logger.println("Exception in Client Thread ", streamId, ":");
			logger.println(e.getMessages());
//			if (e.isPrintStack()) {
			cause.printStackTrace();
//			}
		} catch (BadSetupException e) {
			logger.println(e.toString());
		} finally {
			try {
                manager.clientFinished(this, success);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
		}
	}
	
	public void closeConn() {
		conn.close();
	}
}

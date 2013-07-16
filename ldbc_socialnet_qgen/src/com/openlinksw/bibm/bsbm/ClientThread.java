package com.openlinksw.bibm.bsbm;

import java.util.Locale;
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
import com.openlinksw.util.DoubleLogger;

public class ClientThread implements Runnable {
	private ServerConnection conn;
	protected QueryMixStatistics queryMixStat;
	private ClientManager manager;
	private int nr;
	
	ClientThread(ClientManager parent, int clientNr) {
		this.manager = parent;
		this.nr = clientNr;
		AbstractTestDriver driver=parent.driver;
		int timeoutInSeconds = driver.timeout.getValue()/1000;
        if (driver.isDryMode()) {
            conn = new SQLConnection();
        } else if (driver.doSQL.getValue()) {
	        String driverClassName = driver.driverClassName.getValue();
	        String endPoint = driver.getNextEndPoint(Query.SELECT_TYPE);
			conn = new SQLConnection(driverClassName, endPoint, timeoutInSeconds);
		} else {
			conn = new SPARQLConnection(driver);
	   }
        queryMixStat = new QueryMixStatistics();
	}
	
    private void _run() throws InterruptedException {
		for (;;) {
			if (Thread.interrupted()) throw new InterruptedException();
			CompiledQueryMix queryMix =  manager.getNextQueryMix();

			//Either the warmup querymixes or the run querymixes ended
			if (queryMix==null) break;
            int run = queryMix.getRun();
			long startTime = System.nanoTime();
			for (Object nextQ: queryMix.getQueryMix()) {
                CompiledQuery next = (CompiledQuery) nextQ;
                AbstractQueryResult result = conn.executeQuery(next);
                String qName = next.getName();
                if (result.isTimeout()) {
                    queryMixStat.reportTimeOut(qName);
                } else  if ( result.getQueryMixRun() >= 0) {
                    queryMixStat.setCurrent(qName, result.getResultCount(), result.getTimeInSeconds());
                    manager.addResult(result);
                }
			}
			DoubleLogger.getOut().printf("Thread %d: query mix: %d  %.3f s, total: %.3f s\n", nr, run,
					queryMixStat.getQueryMixRuntime(),	(System.nanoTime()-startTime)/(double)1000000000).flush();
			queryMixStat.finishRun();
		} // for
	}

	@Override
    public void run() {
		DoubleLogger logger = DoubleLogger.getErr();
		try {
			_run();
		} catch (InterruptedException e) {
			logger.println("Client Thread ", nr, " interrupted. Quitting...");
		} catch (ExceptionException e) {
			Throwable cause=e.getCause();
			logger.println("Exception in Client Thread ", nr, ":");
			logger.println(e.getMessages());
//			if (e.isPrintStack()) {
			cause.printStackTrace();
//			}
		} catch (BadSetupException e) {
			logger.println(e.toString());
		} finally {
			manager.finishRun(queryMixStat);
		}
	
	}
	
	public void closeConn() {
		conn.close();
	}
}

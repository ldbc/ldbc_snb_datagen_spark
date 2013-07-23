package com.openlinksw.bibm.bsbm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Locale;

import com.openlinksw.bibm.AbstractClientManager;
import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.CompiledQuery;
import com.openlinksw.bibm.CompiledQueryMix;
import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.Query;
import com.openlinksw.util.DoubleLogger;
import com.openlinksw.util.FiniteQueue;

public class ClientManager extends AbstractClientManager<QueryMixStatistics> implements Runnable {
	private int nrThreads;
	private int nrWarmup;
	private int nrRuns;
    private BSBMQueryMix queryMix;
	private ClientThread[] clients;
	public TestDriver driver;
    private FiniteQueue<CompiledQueryMix> outBuf; // tasks for client threads
	int activeThreadsInRun;
	boolean warmupPhase;
	
//	private LinkedBlockingQueue<QueryMixStatistics> results=new LinkedBlockingQueue<QueryMixStatistics>();
	
	public ClientManager(TestDriver parent) {
		this.driver = parent;
		this.nrWarmup = parent.warmups.getValue();
		this.nrRuns = parent.nrRuns.getValue();
		this.nrThreads = parent.nrThreads.getValue();
		this.queryMix = parent.queryMix;
        queryMixStat = new QueryMixStatistics();
		clients = new ClientThread[this.nrThreads];		
		outBuf=new FiniteQueue<CompiledQueryMix>(new ArrayDeque<CompiledQueryMix>(nrThreads*2)); 
	}
	
	public void createClients() {
		for (int i=0; i<nrThreads; i++) {
			clients[i] = new ClientThread(this, i+1);
		}
	}
	
	private synchronized void startClients() {
		for (int i=0; i<nrThreads;i++) {
			new Thread(clients[i]).start();
		}
		activeThreadsInRun = nrThreads;
	}
	
	private void closeConns() {
		for (int i=0; i<nrThreads;i++) {
			clients[i].closeConn();
		}
	}
	
	/*
	 * warmup run
	 */
	public void doWarmups() throws InterruptedException {
		warmupPhase=true;
		makeRuns(-nrWarmup, 0, driver.warmUpdate.getValue());
        DoubleLogger.getOut().println("Warmup phase ended...\n").flush();
		return;
	}

	/*
	 * start actual run
	 */
	public void doActualRuns() throws InterruptedException {
        DoubleLogger.getOut().println("Starting actual run...").flush();
		warmupPhase=false;

        double totalRunTimeInSeconds = makeRuns(0, nrRuns, true);

		queryMixStat.setElapsedRuntime(totalRunTimeInSeconds);
		DoubleLogger.getOut().printf("Benchmark run completed in %.3f s\n", totalRunTimeInSeconds);
		closeConns();
		return;
	}

	private double makeRuns(int startNr, int endNr, boolean doUpdate) throws InterruptedException {
		int nrRun=startNr;
		for (; nrRun<endNr; nrRun++) {
			CompiledQueryMix item=createCompiledQueriMix(queryMix, nrRun,  doUpdate);
			boolean full=!outBuf.offer(item);
			if (full) break;
		}
        long start = System.nanoTime();
        startClients();
		for (; nrRun<endNr; nrRun++) {
			CompiledQueryMix item=createCompiledQueriMix(queryMix, nrRun,  doUpdate);
			outBuf.add(item);
		}
		outBuf.setFinish();
		waitClients();
        long stop = System.nanoTime();
        return (stop - start)/(double)1000000000;
	}

    public Object[] getParametersForQuery(Query query, int level) {
        if (driver.useDefaultParams.getValue()) {
            FormalParameter[] fps=query.getFormalParameters();
            int paramCount=fps.length;
            String[] parameters = new String[paramCount];
            
            for (int i=0; i<paramCount; i++) {
                FormalParameter fp=(BSBMFormalParameter) fps[i];
                parameters[i] = fp.getDefaultValue();
            }
            return parameters;
        } else {
            return driver.parameterPool.getParametersForQuery(query, level);
        }
    }
    
	public CompiledQueryMix createCompiledQueriMix(BSBMQueryMix queryMix, int nrRun, boolean doUpdates) {
		Query[] queries = queryMix.getPermutatedQueries();
		ArrayList<Object> queryMixRun = new ArrayList<Object>();
		for (Query next: queries) {
		    if (next==null) continue;
			// by default, don't create update queries for the warm-up phase
			byte queryType = next.getQueryType();
			if (!doUpdates && queryType==Query.UPDATE_TYPE) {
				queryMixRun.add(null);
				continue;
			}
			int minLevel=-1, maxLevel=-1;
			if (driver.drillDown.getValue()) {
				ProdTypeLevelRange range = next.getProdTypeLevelRange();
				if (range!=null) {
				    minLevel=range.minLevel;
				    maxLevel=range.maxLevel;
				}
			}
			for (int level=minLevel; level<=maxLevel; level++) {
				Object[] params = getParametersForQuery(next, level);
                CompiledQuery compQuery = new CompiledQuery(next, params, nrRun);
				queryMixRun.add(compQuery);
			}
		}
		CompiledQueryMix cqueryMix=new CompiledQueryMix(queryMixRun, nrRun);
		return cqueryMix;
	}

	private synchronized void waitClients() {
		while(activeThreadsInRun>0) {
			try {
				wait();
			}	catch(InterruptedException e) {
			    DoubleLogger.getErr().println("ClientManager interrupted. Exit.");
				break;
			}
		}
	}

	/*
	 * If a client is finished it reports its results to the ClientManager
	 */
	public synchronized void finishRun(QueryMixStatistics qMixStat) {
		if (!warmupPhase) {
			queryMixStat.addMixStat(qMixStat);
		}
		activeThreadsInRun--;
		notifyAll();
	}

	public CompiledQueryMix getNextQueryMix() throws InterruptedException {
		return outBuf.take();
	}

    public void addResult(AbstractQueryResult result) {
       try {
          driver.resultQueue.add(result);
       } catch (InterruptedException e) {
           // this cannot happen: the queue is unbounded
           e.printStackTrace();
       }
    }

    public void run()  {
        try {
            createClients();
            DoubleLogger.getOut().println("-- preparation time=", (System.currentTimeMillis()-driver.startTime)).flush();
            doWarmups();
            driver.sutStart();
            doActualRuns();
            driver.sutEnd();
            queryMixStat.fillFrame(true, driver, queryMix.getQueries());
        } catch (InterruptedException e) {
        } finally {
            outBuf.setFinish();
            driver.resultQueue.setFinish();
        }
    }
}
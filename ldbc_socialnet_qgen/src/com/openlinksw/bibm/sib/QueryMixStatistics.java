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
package com.openlinksw.bibm.sib;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;

import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.statistics.AbstractQueryMixStatistics;
import com.openlinksw.bibm.statistics.Element;
import com.openlinksw.bibm.statistics.Frame;
import com.openlinksw.bibm.statistics.FrameSchema;
import com.openlinksw.util.Util;

public class QueryMixStatistics extends Frame implements AbstractQueryMixStatistics {
	protected HashMap<String, QueryStatistics> queryStatsByName;
	int queryNr;
	
	private int queryMixRuns;//number of query mix runs
	private double minQueryMixRuntime = Double.MAX_VALUE;
	private double maxQueryMixRuntime = 0;
	private double queryMixRuntime;//whole runtime of actual run in seconds
	private double queryMixGeoMean;
	private double totalRuntime;//Total runtime of all runs
	private double elapsedRuntime;//ClientManager sets this value after the runs
	
	public QueryMixStatistics() {
		super(schema);
		queryStatsByName=new HashMap<String, QueryStatistics>();
	}

    protected QueryStatistics getQueryStat(String qName) {
        QueryStatistics res=queryStatsByName.get(qName);
        if (res==null) {
            res=new QueryStatistics(qName);
            queryStatsByName.put(qName, res);
        }
        return res;
    }
    
	/*
	 * Calculate metrics for this run
	 */
	public void finishRun() {
		queryMixRuns++;
		
		if(queryMixRuntime < minQueryMixRuntime)
			minQueryMixRuntime = queryMixRuntime;
		
		if(queryMixRuntime > maxQueryMixRuntime)
			maxQueryMixRuntime = queryMixRuntime;
		
		queryMixGeoMean += Math.log10(queryMixRuntime);
		totalRuntime += queryMixRuntime;
		elapsedRuntime=totalRuntime;

		//Reset queryMixRuntime
		queryMixRuntime = 0;
   }
	
	/*
	 * Add results of a CompiledQueryMix
	 */
    public synchronized void addMixStat(QueryMixStatistics stat) {
        HashMap<String, QueryStatistics> queryStatsByName2 = stat.getQueryStatsByName();
        for (String qName: queryStatsByName2.keySet()) {
            QueryStatistics qs=getQueryStat(qName);
            QueryStatistics qs1=queryStatsByName2.get(qName);
            qs.addStat(qs1);
        }
		totalRuntime+=stat.getTotalRuntime();
		queryMixGeoMean += stat.getQueryMixGeoMean();
		queryMixRuns+=stat.getQueryMixRuns();;
		
		double cMinQueryMixRuntime = stat.getMinQueryMixRuntime();
		if (cMinQueryMixRuntime < minQueryMixRuntime)
			minQueryMixRuntime = cMinQueryMixRuntime;
		
		double cMaxQueryMixRuntime = stat.getMaxQueryMixRuntime();
		if (cMaxQueryMixRuntime > maxQueryMixRuntime)
			maxQueryMixRuntime = cMaxQueryMixRuntime;
	}
	
	public void reportTimeOut(String qName ) {
		getQueryStat(qName).reportTimeOut();
	}
	
	/*
	 * Set the time (seconds) of the current Query
	 */
	public void setCurrent(String qName, int numberResults, double timeInSeconds) {
        getQueryStat(qName).setCurrent(numberResults, timeInSeconds);
		queryMixRuntime+=timeInSeconds;
	}
	
	/**
	 * average query execution time using geometric mean
	 * @return
	 */
	public double getAQETg() {
		double tmp=0;
		int cnt=0;
		for (QueryStatistics qs: queryStatsByName.values()) {
			int runsPQ = qs.getRunsPerQuery();
			if (runsPQ==0) continue;
			tmp+=  qs.getAqetg()/runsPQ;
			cnt++;
		}
		return Math.pow(10, tmp/cnt);
	}
	
	public double getPower(double scaleFactor) {
		return (3600/getAQETg())*scaleFactor;
	}

	public double getThroughput(double scaleFactor) {
		int queryCount=0;
		for (QueryStatistics qs: queryStatsByName.values()) {
			queryCount +=qs.getRunsPerQuery();
		}
		return (3600.0*queryCount/elapsedRuntime)*scaleFactor;
	}
/*
	public double[] getGeoMean() {
		double[] temp = new double[queryStats.length];
		for (int k=0; k<queryStats.length; k++) {
			QueryStatistics qs=queryStats[k];
			int runsPQ = qs.getRunsPerQuery();
		    if (runsPQ==0) continue;
		    temp[k] = Math.pow(10, qs.getAqetg()/runsPQ);
		}
		return temp;
	}
*/
	public void fillFrame(boolean all, TestDriver dr, HashMap<String,Query> queries) {
		setAttr("version", dr.version);
		addElem("date", dr.date.toString());
		double scalefactor = dr.parameterPool.getScalefactor();
        addElem("scalefactor", scalefactor);
		String[] endPoints = dr.getSparqlEndpoints();
		if (endPoints!=null) {
			for (String point: endPoints) {
				addElem("exploreEndpoint", point);			
			}
		}
		String[] uendPoints = dr.getSparqlEndpoints();
		if (uendPoints!=null) {
			for (String point: uendPoints) {
				addElem("updateEndpoint", point);			
			}
		}
		for (File uscaseDir: dr.querymixDirs) {
			addElem("usecase", uscaseDir.getAbsolutePath());			
		}
		if (dr.retryErrorMessage.getValue()!=null && dr.numRetries.getValue()>0) {
			addElem("retry", dr.numRetries.getValue());			
		}
		addElem("warmups", dr.warmups.getValue());
		int nThreads = dr.nrThreads.getValue();
        if (nThreads>1) {
			addElem("nrthreads", nThreads);
		}
		addElem("seed", dr.seed.getValue());
		addElem("querymixruns", queryMixRuns);
		String minqrt = String.format(Locale.US, "%.4f", minQueryMixRuntime);
		addElem("minquerymixruntime", minqrt);
		String maxqrt = String.format(Locale.US, "%.4f", maxQueryMixRuntime);
		addElem("maxquerymixruntime", maxqrt);
		addElem("#minmaxquerymixruntime", minqrt+"s / "+maxqrt+'s');

		addElem("elapsedruntime", String.format(Locale.US, "%.3f", elapsedRuntime));
		double singleMultiRatio = 1.0;
		if (nThreads>1) {
			addElem("totalruntime", String.format(Locale.US, "%.3f", totalRuntime));
			singleMultiRatio = totalRuntime / elapsedRuntime;
		}
		Double sut=dr.getSUT();
		if (sut!=null) {
			String serverseconds = String.format(Locale.US,	"%.3f", sut);
			addElem("serverruntime", serverseconds);			
			addElem("#serverruntime", String.format(Locale.US,	"%s seconds; %.3f%%", serverseconds , sut*100/elapsedRuntime));			
		}

		addElem("qmph", String.format(Locale.US, "%.3f", getQmph()));
		addElem("cqet",  String.format(Locale.US, "%.3f", getCQET()));
		addElem("cqetg", String.format(Locale.US, "%.3f", getQueryMixGeometricMean()));
		addElem("aqetg", String.format(Locale.US, "%.3f", getAQETg()));
		addElem("throughput", String.format(Locale.US, "%.3f", getThroughput(scalefactor)));
		addElem("power", String.format(Locale.US, "%.3f", getPower(scalefactor)));
		
		if (all) {
		    Collection<QueryStatistics> collection = queryStatsByName.values();
		    QueryStatistics[] qStatsSorted = (QueryStatistics[]) collection.toArray(new QueryStatistics[collection.size()]);
            Util.sortNameable(qStatsSorted);
	        for (QueryStatistics stat: qStatsSorted) {
                if (stat.getRunsPerQuery()==0) continue;
                Query query = queries.get(stat.qrName);
                byte queryType = query==null?0:query.getQueryType();
                stat.fillFrame(totalRuntime, singleMultiRatio, queryType);                  
                addElem(stat);
			}
		}
	}

	public String toXML() {
		StringBuilder sb = new StringBuilder(100);
		sb.append("<?xml version=\"1.0\"?>\n");
		sb.append("<bdsm version=\"").append(getAttr("version")).append("\">\n");

		sb.append("  <querymix>\n");
		for (Element elem: elems) {
			if (! (elem instanceof Frame)) {
				if (elem.schema.getName().startsWith("#")) continue; // this element is for printing only
				sb.append("     "+elem.toString()); // FIXME			
			}
		}
		sb.append("  </querymix>\n");

	    sb.append("  <queries>\n");
		for (Element elem: elems) {
			if (elem instanceof QueryStatistics) {
				((QueryStatistics)elem).toXML(sb);
			}
		}
	    sb.append("  </queries>\n");
	    
		sb.append("</bdsm>\n");
		return sb.toString();
	}

    public HashMap<String, QueryStatistics> getQueryStatsByName() {
        return queryStatsByName;
    }

	public double getQueryMixRuntime() {
		return queryMixRuntime;
	}

	public int getQueryMixRuns() {
		return queryMixRuns;
	}

	public double getQmph() {
		return queryMixRuns * 3600 / elapsedRuntime;
	}

	public double getTotalRuntime() {
		return totalRuntime;
	}
	
	public double getCQET() {
		return totalRuntime / queryMixRuns;
	}

	public double getMinQueryMixRuntime() {
		return minQueryMixRuntime;
	}

	public double getMaxQueryMixRuntime() {
		return maxQueryMixRuntime;
	}

	public double getQueryMixGeometricMean() {
		return Math.pow(10, (queryMixGeoMean/queryMixRuns));
	}

	public double getQueryMixGeoMean() {
		return queryMixGeoMean;
	}

	public double getElapsedRuntime() {
		return elapsedRuntime;
	}

	public void setElapsedRuntime(double elapsedRuntime) {
		this.elapsedRuntime = elapsedRuntime;
	}

	public String getResultString() {
		StringBuilder sb = new StringBuilder(100);
		sb.append("\nVersion:                ").append(getAttr("version")).append('\n');
		print(sb);
		return sb.toString();
	}

	static String[][] cellsDescriptions={
		{"date", "Start date:             "},
		{"scalefactor", "Scale factor:           "},
		{"exploreEndpoint", "Explore Endpoint:       "},
		{"updateEndpoint", "Update Endpoint:        "},
		{"usecase",             "Use case:               "},
		//"Max retries:            " TODO
		{"drilldown", "Drilldown:              "},
		{"warmups", "Number of warmup runs:  "},
		{"nrthreads", "Number of clients:      "},
		{"seed", "Seed:                   "},
		{"querymixruns", "Number of query mix runs (without warmups): ", " times"},
		{"minquerymixruntime"},
		{"maxquerymixruntime"},
		{"#minmaxquerymixruntime", "min/max Querymix runtime:	"},
		{"elapsedruntime", "Elapsed runtime:        ", " seconds"},
		{"totalruntime",      "Total runtime (sum):    ", " seconds"},
		{"serverruntime"},
		{"#serverruntime", "Server runtime:	        "},
		{"qmph", "QMpH:                   ", " query mixes per hour"},
		{"cqet", "CQET:                   ", " seconds average runtime of query mix"},
		{"cqetg", "CQET (geom.):           ", " seconds geometric mean runtime of query mix"},
		{"aqetg", "AQET (geom.):           ", " seconds geometric mean runtime of query"},
		{"throughput", "TPC-H Throughput:       ", " qph*scale"},
		{"power", "TPC-H Power:            ", " qph*scale (geom)"},
   };
	static FrameSchema schema=new FrameSchema("querymix", cellsDescriptions);

}

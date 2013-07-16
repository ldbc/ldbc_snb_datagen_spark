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
package com.openlinksw.bibm.tpch;

import java.util.Locale;

import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.statistics.Element;
import com.openlinksw.bibm.statistics.Frame;
import com.openlinksw.bibm.statistics.FrameSchema;
import com.openlinksw.util.Nameable;

/**
 * reflects results of all executions of a given query
 * @author ak
 *
 */
public class QueryStatistics extends Frame implements Nameable {
	String qrName; // query number, starting from 1
	
	private double aqet;//arithmetic mean query execution time
	private double qmin = Double.MAX_VALUE;
	private double qmax=Double.MIN_VALUE;//Query maximum execution time
	private double avgResults;
	private double aqetg;//Query geometric mean execution time
	private int minResults = Integer.MAX_VALUE;
	private int maxResults = Integer.MIN_VALUE;
	private int runsPerQuery;//Runs Per Query
	private int timeoutsPerQuery;

	public QueryStatistics(String qrName) {
		super(schema);
		this.qrName = qrName;
	}

    @Override
    public String getName() {
        return qrName;
    }
    
	/*
	 * Set the time (seconds) of the current Query
	 */
	public void setCurrent(int numberResults, double timeInSeconds) {		
		int nrRuns = runsPerQuery++;
		aqet = (aqet * nrRuns + timeInSeconds) / (nrRuns+1);
		avgResults = (avgResults * nrRuns + numberResults) / (nrRuns+1);
		aqetg += Math.log10(timeInSeconds);
		
		if(timeInSeconds < qmin)
			qmin = timeInSeconds;
		
		if(timeInSeconds > qmax)
			qmax = timeInSeconds;
		
		if(numberResults < minResults)
			minResults = numberResults;
		
		if(numberResults > maxResults)
			maxResults = numberResults;
	}
	
	public void reportTimeOut() {
		timeoutsPerQuery++;
	}

	/**
	 * @return  arithmetic average query execution time
	 */
	public double getAqet() {
		return aqet;
	}

	/**
     * @return Query minimum execution time
	*/
	public double getQmin() {
		return qmin;
	}

    /**
     * @return Query maximum execution time
    */
	public double getQmax() {
		return qmax;
	}

	public int getRunsPerQuery() {
		return runsPerQuery;
	}

	public double getAvgResults() {
		return avgResults;
	}

	public int getMinResults() {
		return minResults;
	}

	public int getMaxResults() {
		return maxResults;
	}

	public double getGeoMean() {
		return Math.pow(10, aqetg/runsPerQuery);
	}

    /**
     * @return  geometric average query execution time
     */
	public double getAqetg() {
		return aqetg;
	}

	public int getTimeoutsPerQuery() {
		return timeoutsPerQuery;
	}

	public void fillFrame(double totalRuntime, double singleMultiRatio, 	byte queryType) {
		setAttr("nr", qrName);
		addElem("executecount", runsPerQuery);
		if (runsPerQuery==0) {
			addElem("aqet", "0.0");
			return;
		}
		double timeshare = 100*aqet*runsPerQuery/totalRuntime;
		addElem("timeshare", String.format(Locale.US, "%.3f", timeshare));
		addElem("aqet", String.format(Locale.US, "%.6f", aqet));
		addElem("aqetg", String.format(Locale.US, "%.6f", getGeoMean()));
		addElem("qps", String.format(Locale.US, "%.3f", singleMultiRatio / aqet));
		String minqet = String.format(Locale.US, "%.6f", qmin);
		addElem("minqet", minqet);
		String maxqet = String.format(Locale.US, "%.6f", qmax);
		addElem("maxqet",  maxqet);
		addElem("#minmaxqet",  minqet+"s / "+maxqet+'s');
		String avgresults = String.format(Locale.US, "%.3f", avgResults);
		addElem("avgresults",  avgresults);
		addElem("minresults", minResults);
		addElem("maxresults", maxResults);
		if (queryType == Query.SELECT_TYPE) {
			addElem("#avgresultsC",  avgresults);
			addElem("#minmaxresultsC", "" + minResults+" / "+maxResults);
		} else {
			addElem("#avgresultsB",  avgresults);
			addElem("#minmaxresultsB", "" + minResults+" / "+maxResults);
		}
		addElem("timeoutcount", timeoutsPerQuery);
	}

	void toXML(StringBuilder sb) {
		sb.append("    <query nr=\"").append(attrs.get("nr")).append("\">\n");
		for (Element qelem: elems) {
			if (qelem.schema.getName().startsWith("#")) continue; // this element is for printing only
			sb.append("      ").append(qelem.toString()); // FIXME			
		}
		sb.append("    </query>\n");
	}
	
	@Override
	public void print(StringBuilder sb) {
		sb.append("Metrics for Query:      ").append(qrName).append('\n');
		for (Element elem: elems) {
			if (! (elem instanceof Frame)) {
				elem.print(sb); 			
			}
		}
	}

	static String[][] cellsDescriptions={
		{"executecount", "Count:                  ", " times executed in whole run"},
		{"timeshare", "Time share              ", "% of total execution time"},
		{"aqet", "AQET:                   ", " seconds (arithmetic mean)"},
		{"aqetg", "AQET(geom.):            ", " seconds (geometric mean)"},
		{"qps", "QPS:                    ", " Queries per second"},
		{"minqet"},
		{"maxqet"},
		{"#minmaxqet", "minQET/maxQET:          "},
		{"minresults"},
		{"maxresults"},
		{"#minmaxresultsC", "min/max result count:   "},
		{"#minmaxresultsB", "min/max result (Bytes): "},
		{"avgresults"},
		{"#avgresultsC", "Average result count:   "},
		{"#avgresultsB", "Average result (Bytes): "},
		{"timeoutcount", "Number of timeouts:     "},
   };
	
	static FrameSchema schema=new FrameSchema("query", cellsDescriptions);
}

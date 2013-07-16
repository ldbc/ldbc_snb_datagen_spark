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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;

import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.statistics.AbstractQueryMixStatistics;
import com.openlinksw.bibm.statistics.Element;
import com.openlinksw.bibm.statistics.Frame;
import com.openlinksw.bibm.statistics.FrameSchema;
import com.openlinksw.bibm.tpch.StreamStatistics.QueryStat;

/**
 * reflects results of execution of one run of one query stream
 * @author ak
 *
 */
public class QueryMixStatistics extends Frame implements AbstractQueryMixStatistics {
    TestDriver dr;
    protected HashMap<Integer, StreamStatistics> streamStats;
	protected HashMap<String, QueryStatistics> queryStatsByName;
	
    private double measurmentInterval; // elapsed run time of the throughput test, in seconds
	
	public QueryMixStatistics(TestDriver dr) {
		super(schema);
        this.dr = dr;
		streamStats=new HashMap<Integer, StreamStatistics>();
		queryStatsByName=new HashMap<String, QueryStatistics>();
	}

    private QueryStatistics getQueryStat(String qName) {
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
	    StreamStatistics refreshStream=streamStats.get(-1);
        ArrayList<ExternalQuery> rqstats = null;
	    for (int streamId=1; streamId<=dr.nClients; streamId++) {
	        StreamStatistics strStat=streamStats.get(streamId);
	        if (strStat==null) continue;
            for (QueryStat qStat: strStat.getqStats()) {
                getQueryStat(queryName(qStat.qName)).setCurrent(qStat.numberResults, qStat.timeInSeconds);
            }
	    }
        if (refreshStream!=null) {
            rqstats = refreshStream.getRfStats();
            for (ExternalQuery rf: rqstats) {
                StreamStatistics strStat=streamStats.get(rf.streamId);
                if (strStat==null) continue;
                strStat.rfs[rf.name-1]=rf;
                getQueryStat(queryName(22+rf.name)).setCurrent(0, rf.getTimeiseconds());
            }
        }	    
   }
	
	/**
	 * Add results of another CompiledQueryMix
	 */
    public synchronized void addStreamStat(StreamStatistics streamStat) {
        streamStats.put(streamStat.getStreamId(), streamStat);
	}
	
	public void reportTimeOut(String qName ) {
		getQueryStat(qName).reportTimeOut();
	}
	
	/*
	 * Set the time (seconds) of the current Query
	 */
	public void setCurrent(String qName, int numberResults, double timeInSeconds) {
        getQueryStat(qName).setCurrent(numberResults, timeInSeconds);
	}
	
	public void setMeasurmentInterval(double measurmentInterval) {
        this.measurmentInterval = measurmentInterval;
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
		return (3600.0*queryCount/measurmentInterval)*scaleFactor;
	}

	@SuppressWarnings("static-access")
    public void fillFrame(boolean all, HashMap<String,Query> queries) {
	    finishRun();
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
        addElem("usecase", dr.uscaseDir.getAbsolutePath());         
		if (dr.retryErrorMessage.getValue()!=null && dr.numRetries.getValue()>0) {
			addElem("retry", dr.numRetries.getValue());			
		}
        addElem("nStreams", dr.nClients);
		addElem("seed", dr.seed.getValue());

        double power = getPower(scalefactor);
        double throughput = getThroughput(scalefactor);
        addElem("power", String.format(Locale.US, "%.1f", power));
        addElem("throughput", String.format(Locale.US, "%.1f", throughput));
        addElem("qmph", String.format(Locale.US, "%.1f", Math.sqrt(power*throughput)));
        addElem("elapsedruntime",  String.format(Locale.US, "%.1f", measurmentInterval));
	}

	public String toXML() {
		StringBuilder sb = new StringBuilder(100);
		sb.append("<?xml version=\"1.0\"?>\n");
		sb.append("<bibm version=\"").append(getAttr("version")).append("\">\n");

		sb.append("  <querymix>\n");
		for (Element elem: elems) {
			if (! (elem instanceof Frame)) {
				if (elem.schema.getName().startsWith("#")) continue; // this element is for printing only
				sb.append("     "+elem.toString()); // FIXME			
			}
		}
		sb.append("  </querymix>\n");
/*
	    sb.append("  <queries>\n");
		for (Element elem: elems) {
			if (elem instanceof QueryStatistics) {
				((QueryStatistics)elem).toXML(sb);
			}
		}
	    sb.append("  </queries>\n");
	    */
		sb.append("</bibm>\n");
		return sb.toString();
	}

	public String getResultString() {
		StringBuilder sb = new StringBuilder(100);
		sb.append("\nVersion:                ").append(getAttr("version")).append('\n');
		print(sb);
        sb.append('\n');

        sb.append("Duration of stream execution:\n");
        sb.append("                Query Start         Query End       Duration     RF1 Start           RF2 End           RF2 Start          RF2 End  \n");
        for (int streamId=0; streamId<=dr.nClients; streamId++) {
            StreamStatistics strStat=streamStats.get(streamId);
            if (strStat==null) continue;
            strStat.printDuration(sb);
            sb.append('\n');
       }
        sb.append('\n');

        sb.append("TPC Timing intervals (in seconds):\n");
        sb.append('\n');
        printPartTable(sb, 1, 8);
        printPartTable(sb, 9, 16);
        printPartTable(sb, 17, 24);
		return sb.toString();
	}

    private void printPartTable(StringBuilder sb, int from, int to) {
        sb.append("Query\t\t");
        for (int q=from; q<=to; q++) {
            String qName = queryName(q);
            sb.append(qName).append('\t');
        }
        sb.append('\n');

        for (int streamId=0; streamId<=dr.nClients; streamId++) {
            StreamStatistics strStat=streamStats.get(streamId);
            if (strStat==null) continue;
            strStat.printTiming(sb, from, to);
            sb.append('\n');
        }

        int width = to-from+1;
        Double[] minimuns=new Double[width];
        Double[] maximums=new Double[width];
        Double[] averages=new Double[width];
        for (int k=0; k<width; k++) {
            String qName=queryName(k+from);
            QueryStatistics queryStatistics = queryStatsByName.get(qName);
            if (queryStatistics!=null) {
                minimuns[k]=queryStatistics.getQmin();
                maximums[k]=queryStatistics.getQmax();
                averages[k]=queryStatistics.getAqet();
            }
        }
               
        printAggregate(sb, "Minimum: ", minimuns);
        printAggregate(sb, "Maximum: ", maximums);
        printAggregate(sb, "Average: ", averages);

        sb.append('\n');
    }

    private String queryName(int qNum) {
        if (qNum<=22) {  // FIXME
            return "Q"+qNum;
        } else {
            return "RF"+(qNum-22); 
        }
    }

    private String queryName(String qName) {
        try {
            return queryName(Integer.parseInt(qName));
        } catch (NumberFormatException e) {
           return qName;
        }
    }

    private void printAggregate(StringBuilder sb, String header, Double[] values) {
        sb.append(header).append('\t');
        for (int k=0; k<values.length; k++) {
            Double timing=values[k];
            sb.append(timing==null?"    ":QueryTiming.getTiming(timing)).append('\t');
        }
        sb.append('\n');
    }

	static String[][] cellsDescriptions={
		{"date", "Start date:             "},
		{"scalefactor", "Scale factor:           "},
		{"exploreEndpoint", "Explore Endpoint:       "},
		{"updateEndpoint", "Update Endpoint:        "},
		{"usecase",             "Use case:               "},
		//"Max retries:            " TODO
		{"nStreams", "Query Streams of Throughput Test:"},
		{"seed", "Seed:                   "},
		{"elapsedruntime", "Measurment Interval:    ", " seconds"},
//		{"serverruntime"},
//		{"#serverruntime", "Server runtime:	        "},
		{"qmph", "TPC-H Composite:        "},
		{"throughput", "TPC-H Throughput:       ", " qph*scale"},
		{"power", "TPC-H Power:            ", " qph*scale (geom)"},
   };
	static FrameSchema schema=new FrameSchema("querymix", cellsDescriptions);

}

class Durations extends Frame {

    public Durations() {
        super(schema);
    }
    
    static FrameSchema schema=new FrameSchema("Duration", new  String[0][0]);
}

class DurationStream extends Frame {

    public DurationStream() {
        super(schema);
    }
    
    static FrameSchema schema=new FrameSchema("Duration", new  String[0][0]);
}
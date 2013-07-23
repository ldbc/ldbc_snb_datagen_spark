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

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * reflects results of all executions of a given query stream
 * @author ak
 *
 */
public class StreamStatistics  {
    int streamId; 
    private long  qStart; // start time, millis
    private long  qEnd; // end time, millis
    HashMap<String, QueryStat> qStats=new HashMap<String, QueryStat>();
    ExternalQuery[] rfs=new ExternalQuery[2];  // for own functions
    ArrayList<ExternalQuery> rfStats=new ArrayList<ExternalQuery>(); // used in refresh stream only
    
	public StreamStatistics(int streamId) {
		this.streamId = streamId;
	}

	/*
	 * Set the time (seconds) of the current Query
	 */
	public void setCurrent(String qName, int numberResults, double timeInSeconds) {
	    QueryStat qS=new QueryStat(qName, numberResults, timeInSeconds);
	    qStats.put(qName, qS);
	}

	public void reportTimeOut(String qName, double timeInSeconds) {
        QueryStat qS=new QueryStat(qName, -1, timeInSeconds);
        qStats.put(qName, qS);
	}
	
	public int getStreamId() {
        return streamId;
    }

    public void setqStart(long qStart) {
        this.qStart = qStart;
    }

    public void setqEnd(long qEnd) {
        this.qEnd = qEnd;
    }

    public Collection<QueryStat> getqStats() {
        return qStats.values();
    }
	
	public ArrayList<ExternalQuery> getRfStats() {
        return rfStats;
    }

    public void printDuration(StringBuilder sb) {
		sb.append("Stream ").append(streamId).append(":\t");
        sb.append(toDateTime(qStart)).append('\t');
        sb.append(toDateTime(qEnd)).append('\t');
        sb.append(toTimeInterval(qStart, qEnd)).append('\t');
        ExternalQuery rf=rfs[0];
        if (rf==null) {
            return;
        }
        sb.append(toDateTime(rf.start)).append('\t');
        sb.append(toDateTime(rf.end)).append('\t');
        rf=rfs[1];
        if (rf==null) {
            return;
        }
        sb.append(toDateTime(rf.start)).append('\t');
        sb.append(toDateTime(rf.end)).append('\t');
	}
    
    public void printTiming(StringBuilder sb, int from, int to) {
        sb.append("Stream ").append(streamId).append(":\t");
        for (int k=from; k<=to; k++) {
            QueryTiming timing=null;
            if (k<=22) { // FIXME
                String name=Integer.toString(k);
                timing = qStats.get(name);
            } else {
                timing = rfs[k-23]; // FIXME
            }
            sb.append(timing==null?"    ":timing.getTiming()).append('\t');
        }
    }

    static String toTimeInterval(long start, long end) {
        long sec=(long) ((end-start)/1000d);
        long mins=sec/60; sec-=mins*60;
        long hrs=mins/60; mins-=hrs*60;
        return String.format("%02d:%02d:%02d", hrs, mins, sec);
    }
    
    static String toDateTime(long date) {
        SimpleDateFormat ft = new SimpleDateFormat ("dd/MM/yyyy hh:mm:ss");
        return ft.format(new Date(date));
    }    
    
    static class QueryStat extends QueryTiming {
        String qName; // starting from 1
        int numberResults;
        double timeInSeconds;

        public QueryStat(String qName, int numberResults, double timeInSeconds) {
            this.qName = qName;
            this.numberResults = numberResults;
            this.timeInSeconds = timeInSeconds;
        }

        @Override
        public String getTiming() {
            return getTiming(timeInSeconds);
         }
    }

    public void addRF(ExternalQuery rf) {
        if (rf.streamId==this.streamId) {
            rfs[rf.name-1]=rf;
        } else {
            rfStats.add(rf);
        }
    }

}

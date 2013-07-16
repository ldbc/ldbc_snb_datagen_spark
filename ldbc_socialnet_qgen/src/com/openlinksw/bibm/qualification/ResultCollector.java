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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import com.openlinksw.bibm.AbstractTestDriver;
import com.openlinksw.bibm.Query;
import com.openlinksw.util.Util;
import com.openlinksw.util.json.WriterPrinter;

/**
 * generates qualification file
 * new ResultCollector()->addResultDescriptions<>()->[addResult()]->close()
 * @author ak
 *
 */
public class ResultCollector extends WriterPrinter {
	boolean finish=false;
	
	public ResultCollector(File qualificationFile) throws IOException  {
	    super (qualificationFile);
        startObject();
	}

    public void addResultDescriptionsFromDriver(AbstractTestDriver driver) {
        comment("Results of test run");
        addEntry("date", new Date().toString());
        Collection<Query> collection=driver.getQueries();
        Query[] sortedQueries = (Query[]) collection.toArray(new Query[collection.size()]);
        Util.sortNameable(sortedQueries);
        startResultDescriptors();
        for (Query query: sortedQueries) {
            if (query==null) continue;
            addResultDescription(query);
        }
        endResultDescriptors();
        startAllResults();
    }

    protected void startAllResults() {
        startEntry("allResults");
        startList();
    }

    protected void endResultDescriptors() {
        endEntry("resultDescriptors");
    }

    protected void startResultDescriptors() {
        startEntry("resultDescriptors");
        startList();
    }

    protected void addResultDescription(Query query) {
        ResultDescription[] resultDescriptions = query.getResultDescriptions();
        if (resultDescriptions==null) return;
        String name = query.getName();
        startObject();
        addEntry("queryName", name);       
        addEntry("results", resultDescriptions);
        Integer[] resultKeys = query.getResultKeys();
        if (resultKeys!=null) {
            addEntry("resultKeys", resultKeys);
        }
        endElement();  // pair to startObject()
    }

    public void addResult(QueryResult qr) throws IOException {
       this.walk(qr);
       endElement();
       super.flush();
    }

    public synchronized void close() throws IOException {
        endEntry("allResults");
        end();
        comment("end");
        wr.close();
    }

}

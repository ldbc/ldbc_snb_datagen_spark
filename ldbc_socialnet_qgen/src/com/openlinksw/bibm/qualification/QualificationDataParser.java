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

import com.openlinksw.util.Nameable;
import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.IndexedJsonList;
import com.openlinksw.util.json.impl.Sequence;
import com.openlinksw.util.json.impl.SimpleJsonObject;

/** represents qualification file 
 *  passes all encountered QueryResults to the method addQueryResult() 
 * @author ak
 *
 */
public abstract class QualificationDataParser extends SimpleJsonObject {
    public ResultDescriptors resultDescriptors;
    ResultContainer allResults;

    public QualificationDataParser() {
        super(true);
    }

    public QualificationDataParser(boolean ignoreCase) {
        super(ignoreCase);
        // TODO Auto-generated constructor stub
    }

    public abstract void addQueryResult(QueryResult result);

    @Override
    public JsonList<?> newJsonList(String key) {
        if (key.equalsIgnoreCase("resultDescriptors")) {
            return new ResultDescriptors();
        } else  if (key.equalsIgnoreCase("allResults")) {
             return new ResultContainer();
        } else {
            return super.newJsonList(key);
        }
    }

    @Override
    public void put(String key, Object value) {
        if (key.equalsIgnoreCase("resultDescriptors")) {
            resultDescriptors=(ResultDescriptors) value;
        } else  if (key.equalsIgnoreCase("allResults")) {
            allResults=(ResultContainer) value;
        }
    }

    public static class ResultDescriptors extends  IndexedJsonList<Nameable> {

        @Override
        public JsonObject newJsonObject(boolean ignoreCase) {
            return new NamedResultDescriptionLists();
        }

        @Override
        public NamedResultDescriptionLists get(String name) {
            return (NamedResultDescriptionLists) super.get(name);
        }
    }
    
    @Sequence(value={"queryName", "results", "resultKeys"})
    @IgnoreCase
    public static  class NamedResultDescriptionLists extends ResultDescriptionLists implements Nameable {
        String queryName;

        public String getQueryName() {
            return queryName;
        }

        public void setQueryName(String quieryName) {
            this.queryName = quieryName;
        }

        @Override
        public String getName() {
            return queryName;
        }
        
    }
    
    class ResultContainer extends JsonList<QueryResult> {

        @Override
        public JsonObject newJsonObject(boolean ignoreCase) {
            return new QueryResult();
        }

        @Override
        public void add(QueryResult r) {
            NamedResultDescriptionLists resultDescs = resultDescriptors.get(r.getQueryName());
            if (resultDescs!=null) {
                r.setRowDescr(resultDescs);
            }
            addQueryResult(r);
        }

    }
    
}

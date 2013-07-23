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
package com.openlinksw.util.csv2ttl;

import java.io.IOException;

import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.ParserException;
import com.openlinksw.util.json.impl.AutoJsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.IndexedJsonList;
import com.openlinksw.util.json.impl.Sequence;
import com.openlinksw.util.json.impl.SimpleJsonList;

@IgnoreCase
@Sequence(value={"update_header", "header", "default_tag", "tables", "common_fields"})
public class DBSchema extends AutoJsonObject {

    JsonList<String> update_header;
    JsonList<String> header;
	String default_tag; // for AutoJsonObject, name should be the same as in json schema
	private Tables tables;
	CsvFields common_fields;
    String update_header_str;
    String header_str;

    public DBSchema(String filename) throws IOException {
        super(true);
        super.loadFrom(filename);
        for (CsvTableSchema tableScema: tables) {
            tableScema.fixFields(common_fields);
            for (CsvField field: tableScema.getFields()) {
                if (field.refto==null) continue;
                if (tables.get(field.refto)==null) {
                    throw new RuntimeException("Error in "+filename+": field "+tableScema.getName()+"."+field.getName()+" refers to unexistent table '"+field.refto+"'");
                }
            }
        }
    }

    @Override
    public JsonList<?> newJsonList(String key) {
       if ("tables".equalsIgnoreCase(key)) {
            return new Tables();
       } else if ("update_header".equalsIgnoreCase(key)) {
           return  new SimpleJsonList<String>();
       } else if ("header".equalsIgnoreCase(key)) {
           return  new SimpleJsonList<String>();
       } else if ("common_fields".equalsIgnoreCase(key)) {
           return  new CsvFields();
        } else {
            throw new ParserException("Not a List field: '"+key+'\'');
        }
    }

    public String getUpdate_header_str() {
        if (update_header_str==null) {
            StringBuilder sb=new StringBuilder();
            if (update_header!=null) {
                for (String hdr: update_header) {
                    if (hdr.startsWith("\"") || hdr.startsWith("\'")) {
                         sb.append(hdr, 1,  hdr.length()-1);
                    } else {
                        sb.append(hdr);
                    }
                    sb.append('\n');
                }
            }
            update_header_str=sb.toString();
        }
        return update_header_str;
    }

    public String getHeader_str() {
        if (header_str==null) {
            StringBuilder sb=new StringBuilder();
            if (header!=null) {
                for (String hdr: header) {
                    if (hdr.startsWith("\"") || hdr.startsWith("\'")) {
                         sb.append(hdr, 1,  hdr.length()-1);
                    } else {
                        sb.append(hdr);
                    }
                    sb.append('\n');
                }
                header_str=sb.toString();
            }
        }
        return header_str;
    }

    public JsonList<String> getUpdate_header() {
        return update_header;
    }

    public void setUpdate_header(JsonList<String> updateHeader) {
        update_header = updateHeader;
    }

    public JsonList<String> getHeader() {
        return header;
    }

    public void setHeader(JsonList<String> header) {
        this.header = header;
    }

    public String getDefault_tag() {
        return default_tag;
    }

    public void setDefault_tag(String defaultTag) {
        default_tag = defaultTag;
    }

    public Tables getTables() {
        return tables;
    }

    public void setTables(Tables tables) {
        this.tables = tables;
    }

    public CsvFields getCommon_fields() {
        return common_fields;
    }

    public void setCommon_fields(CsvFields commonFields) {
        common_fields = commonFields;
    }

    public CsvTableSchema getTable(String name) {
        return tables.get(name);
    }

    public class Tables extends IndexedJsonList<CsvTableSchema> {

        @Override
        public JsonObject newJsonObject(boolean ignoreCase) {
            return new CsvTableSchema(DBSchema.this);
        }
    }

    static class CsvFields extends IndexedJsonList<CsvField> {

        @Override
        public JsonObject newJsonObject(boolean ignoreCase) {
            return new CsvField();
        }
    }

    static class Row extends SimpleJsonList<String> {
        public Object getTyped(int k) {
            String value=(String)super.get(k);
            if (value.startsWith("\"") || value.startsWith("\'")) {
                return value.substring(1, value.length()-1);
            } else {
                try {
                    return Long.parseLong(value);
                } catch (NumberFormatException e) {
                    return Double.parseDouble(value);
                }
            }
        }
    }
}


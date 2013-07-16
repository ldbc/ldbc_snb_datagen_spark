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

import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.util.Nameable;
import com.openlinksw.util.json.impl.AutoJsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.Sequence;

@IgnoreCase
@Sequence(value={"name", "tag", "type", "prop", "refto"})
public class CsvField extends AutoJsonObject implements Nameable {
    String name;
    String tag;
	DataType type;
	boolean prop=true;
	String refto=null;
	CsvTableSchema referent=null;
	String propString;

	public CsvField() {
    }

    public void fixField(CsvTableSchema csvTableSchema) {
        DBSchema dbSchema = csvTableSchema.getDbSchema();
        if (tag==null) {
            tag=dbSchema.getDefault_tag();
        }
        if (tag!=null) {
            propString=tag+':'+name;
        } else {
            propString=name;
        }
        if (refto!=null) {
            referent=dbSchema.getTable(refto);
            if (referent==null) {
                throw new BadSetupException("In field "+csvTableSchema.getName()+'.'+name+": wrong refto="+refto);
            }
        }
    }

    public Object getPropString() {
        return propString;
    }

    public String makeSubjectString(String value) {
        StringBuilder sb=new StringBuilder();
        if (referent!=null) { // TODO: this works only for simple foreign keys
            sb.append(referent.makeSubjectString(value));
        } else  if (type==DataType.STR) {
            sb.append('\"').append(value).append('\"');
        } else  if (type==DataType.REAL || type==DataType.DOUBLE) {
            // make sure string representation contain exponent, to be distinguished from decimal
            if (!value.contains("E") && !value.contains("e")) {
                sb.append(value).append("e0");
            }
        } else  if (type==DataType.DATE) {
            sb.append('\"').append(value).append("\"^^xsd:dateTime");
        } else {
            sb.append(value);
        }
        return sb.toString();
    }
    
    @Override
    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public void setType(String value) {
        this.type = DataType.valueOf((value).toUpperCase());
    }

    public boolean isProp() {
        return prop;
    }

    public void setProp(String value) {
        this.prop = Boolean.valueOf(value);
    }

    public String getRefto() {
        return refto;
    }

    public void setRefto(String refto) {
        this.refto = refto;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

}

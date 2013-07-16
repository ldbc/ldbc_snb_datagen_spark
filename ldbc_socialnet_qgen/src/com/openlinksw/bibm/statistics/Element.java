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
package com.openlinksw.bibm.statistics;

import java.util.HashMap;
import java.util.Map.Entry;


public abstract class Element {
    public Schema schema;
    protected HashMap<String, String> attrs;
    
    public Element(Schema schema) {
        if (schema==null) throw new NullPointerException();
        this.schema = schema;
    }
    
    public void setAttr(String key, String value) {
        if (attrs==null) {
            attrs=new HashMap<String, String>();
        }
        attrs.put(key, value);
    }


    public void setAttr(String key, int value) {
        setAttr(key, Integer.toString(value));
    }

    public String getAttr(String key) {
        if (attrs==null) {
            return null;
        }
        return attrs.get(key);
    }

    public  void printAttrs(StringBuilder sb) {
        if (attrs!=null && attrs.size()>0) {
            sb.append(" ");
            for (Entry<String, String> attr: attrs.entrySet()) {
                sb.append(" ").append(attr.getKey())
                    .append("=\"").append(attr.getValue()).append('"');
            }
        }
    }

    public abstract void print(StringBuilder sb);
}


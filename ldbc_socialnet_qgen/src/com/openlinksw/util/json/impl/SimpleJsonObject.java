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
package com.openlinksw.util.json.impl;

import java.util.ArrayList;
import java.util.HashMap;

import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;

public class SimpleJsonObject extends  JsonObject {
    protected HashMap<String, Object> map=new HashMap<String, Object>();
    protected ArrayList<String> sequence=new ArrayList<String>();

    public SimpleJsonObject(boolean ignoreCase) {
        super(ignoreCase);
    }

    public SimpleJsonObject() {
    }

    public SimpleJsonObject(SimpleJsonObject other) {
        super(other.ignoreCase);
        this.map = other.map;
        this.sequence = other.sequence;
    }

    @Override
    public void put(String key, Object value) {
        if (ignoreCase) {
            key=key.toLowerCase();
        }
        sequence.add(key);
        map.put(key, value);
    }

    @Override
    public Object get(String key) {
        if (ignoreCase) {
            key=key.toLowerCase();
        }
        return map.get(key);
    }

    public ArrayList<String> printOrder() {
        return sequence;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JsonList<?> newJsonList(String key) {
        return new SimpleJsonList();
    }

    @Override
    public JsonObject newJsonObject(String key, boolean ignoreCase) {
        return new SimpleJsonObject(ignoreCase);
    }

    @Override
    public boolean hasAttr(String key) {
        return true;
    }

}

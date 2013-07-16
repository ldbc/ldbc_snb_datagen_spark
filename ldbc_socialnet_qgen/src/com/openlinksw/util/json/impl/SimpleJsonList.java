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
import java.util.Iterator;

import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;

public class SimpleJsonList<T> extends JsonList<T> {
    ArrayList<T> list=new  ArrayList<T>();

    @Override
    public void add(T e) {
        list.add(e);
    }

    @Override
    public Iterator<T> iterator() {
        return list.iterator();
    }

    @Override
    public JsonList<?> newJsonList() {
        return new SimpleJsonList<Object>();
    }

    @Override
    public JsonObject newJsonObject(boolean ignoreCase) {
        return new SimpleJsonObject(ignoreCase);
    }

    @Override
    public T get(int k) {
        return list.get(k);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public int indexOf(T element) {
        return list.indexOf(element);
    }

    @Override
    public T[] toArray(T[] array) {
        return list.toArray(array);
    }

    @Override
    public void set(int index, T element) {
        list.set(index, element);
    }
}

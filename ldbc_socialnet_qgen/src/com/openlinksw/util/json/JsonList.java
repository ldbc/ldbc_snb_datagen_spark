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
package com.openlinksw.util.json;

import java.util.Iterator;


public abstract class JsonList<T> extends JsonBase implements Iterable<T> {

    /** adds element to the list
     * 
     * @param e
     * @return
     */
    public abstract void add(T e);
    
    public JsonList<?> newJsonList() {
        throw new ParserException(this.getClass().getName()+": List not expected here ");
    }

    public JsonObject newJsonObject(boolean ignoreCase) {
       throw new ParserException(this.getClass().getName()+": Object not expected here");
    }

    public T get(int k) {
       throw new UnsupportedOperationException();
    }

    public void set(int index, T element) {
        throw new UnsupportedOperationException();
    }
    
    public int size() {
        throw new UnsupportedOperationException();
    }

    public T[] toArray(T[] array) {
        throw new UnsupportedOperationException();
    }

    public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    public int indexOf(T element) {
        throw new UnsupportedOperationException();
    }
    
}

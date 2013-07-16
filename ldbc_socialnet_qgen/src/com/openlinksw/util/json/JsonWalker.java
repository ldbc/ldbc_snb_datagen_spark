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

/**
 * Should be named Json Assembler
 * @author ak
 *
 */
public abstract class JsonWalker {

    /**
     * put Object on the stack
     */
    public abstract JsonObject startObject();

    /**
     * general method to create an entry is the following call sequence:
     * startEntry -> (startObject|startList|pushValue) -> endEntry
     */
    public abstract void startEntry(String key);

    /**
     * value is usually a string, that is, neither JsonObject nor JsonList
     * 
     * @param value
     */
    public abstract void pushValue(String value);

    /** key is mentioned second time just for checking
     * 
     * @param key
     */
    public abstract void endEntry(String key);

    public abstract JsonList<?> startList();

    /**
     * general method to create a list element is the following call  sequence:
     *  (startObject|startList|pushValue) -> endElement
     */
    public abstract void endElement();

    /**
     * the end or the root object
     */
    public abstract void end();

    public void walk(Object value) {
        if (value instanceof JsonObject) {
            startObject();
            JsonObject obj=(JsonObject) value;
            for (String key: obj.printOrder()) {
                Object value2 = obj.get(key);
                if (value2==null) {
                    continue;
                }
                startEntry(key);
                walk(value2);
                endEntry(key);
            }
        } else if (value instanceof JsonList<?>) {
            startList();
            JsonList<?> list=(JsonList<?>) value;
            for (Object el: list) {
               walk(el);
               endElement();
            }
        } else if (value instanceof Object[]) {
            startList();
            Object[] list=(Object[]) value;
            for (Object el: list) {
               walk(el);
               endElement();
            }
        } else {
            pushValue(value.toString());
        }
    }

    /**
     * short method to add an entry, if the value is known at the same time as the key
     * 
     * @param key
     * @param value
     */
    public void addEntry(String key, String value) {
        startEntry(key);
        pushValue(value);
        endEntry(key);
    }

    /**
     * short method to add an entry, if the value is known at the same time as the key
     * 
     * @param key
     * @param value
     */
    public void addEntry(String key, JsonList<?> list) {
        startEntry(key);
        walk(list);
        endEntry(key);
    }

    /**
     * short method to add an entry, if the value is known at the same time as the key
     * 
     * @param key
     * @param value
     */
    public void addEntry(String key, Object[] list) {
        startEntry(key);
        walk(list);
        endEntry(key);
    }

    /**
     * short method to add an element, if the value is a String
     * 
     * @param value
     */
    public void addElement(String value) {
        pushValue(value);
        endElement();
    }

}

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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;

import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.util.ArrayIterator;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.ParserException;

/** works via getters and setters
 * 
 * @author ak
 *
 */
public class AutoJsonObject extends JsonObject implements Iterable<String> {
    static HashMap<Class<?>, ClassDescr> classDescrs = new HashMap<Class<?>, ClassDescr>();

    static synchronized ClassDescr getDescr(Class<?> clazz) {
        ClassDescr res = classDescrs.get(clazz);
        if (res == null) {
            res = new ClassDescr(clazz);
            classDescrs.put(clazz, res);
        }
        return res;
    }

    ClassDescr classDescr=getDescr(getClass());

    public AutoJsonObject() {
    }

    public AutoJsonObject(boolean ignoreCase) {
        super(true);
    }

    @Override
    public boolean hasAttr(String attrName) {
        return classDescr.getSetMethod(attrName)!=null;
    }

    @Override
    public Object get(String attrName) {
        Method m = classDescr.getGetMethod(attrName);
        if (m == null) {
            throw new ParserException("No such attribute:" + attrName);
        }
        try {
            return m.invoke(this);
        } catch (InvocationTargetException e) {
            throw new ExceptionException("Problem to read own attribute:" + attrName, e.getCause(), true);
        } catch (Exception e) {
            throw new ExceptionException("Problem to read own attribute:" + attrName, e, true);
        }
    }

    @Override
    public void put(String attrName, Object value) {
        Method m = classDescr.getSetMethod(attrName);
        if (m == null) {
            throw new ParserException("No such attribute:" + attrName);
        }
        try {
            m.invoke(this, value);
        } catch (InvocationTargetException e) {
            throw new ExceptionException("Problem to read own attribute:" + attrName, e.getCause(), true);
        } catch (Exception e) {
            throw new ExceptionException("Problem to write own attribute:" + attrName+" into "+classDescr.getClassName(), e, true);
        }
    }

    @Override
    public Iterable<String> printOrder() {
        return this;
    }

    @Override
    public Iterator<String> iterator() {
        Iterator<String> arrayIterator = new ArrayIterator<String>(classDescr.sequence);
        return arrayIterator;
    }
   
}

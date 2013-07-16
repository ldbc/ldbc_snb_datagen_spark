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

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.TreeSet;

import com.openlinksw.bibm.Exceptions.BadSetupException;

public class ClassDescr {
    String className;
    private HashMap<String, Method> getMethods = new HashMap<String, Method>();
    private HashMap<String, Method> setMethods = new HashMap<String, Method>();
    String[] sequence=null;
    boolean ignoreCase=false;
    TreeSet<String> seqSet=new TreeSet<String>();

    ClassDescr(Class<?> clazz) {
        className=clazz.getName();
        ignoreCase=(clazz.getAnnotation(IgnoreCase.class)!=null);
        Sequence seq = clazz.getAnnotation(Sequence.class);
        if (seq != null) {
            sequence = seq.value();
            for (int k=0; k<sequence.length; k++) {
                String attrName=sequence[k];
                if (ignoreCase) {
                    attrName = attrName.toLowerCase();
                }
                sequence[k]=attrName;
                seqSet.add(attrName);
            }
        }
        for (Method m : clazz.getMethods()) {
            String methodName = m.getName();
            if (methodName.startsWith("is")) {
                if (methodName.length() > 2) {
                    addGetMethod(methodName.substring(2), m);
                }
            } else  if (methodName.startsWith("get")) {
                if (methodName.length() > 3) {
                    addGetMethod(methodName.substring(3), m);
                }
            } else  if (methodName.startsWith("set")) {
                if (methodName.length() > 3) {
                    addSetMethod(methodName.substring(3), m);
                }
            }
        }
        if (sequence == null) {
            sequence = seqSet.toArray(new String[seqSet.size()]);
        } else {
            for (String attr: sequence) {
                if (getMethods.get(attr)==null) {
                    throw new BadSetupException("no get method for attr:"+clazz.getName()+"."+attr); 
                }
                if (setMethods.get(attr)==null) {
                    throw new BadSetupException("no set method for attr:"+clazz.getName()+"."+attr); 
                }
            }
        }
    }
    
    private void addGetMethod( String methodName, Method m) {
        if (m.getParameterTypes().length != 0) {
            return;
        }
         String attrName = Introspector.decapitalize(methodName);
         if (ignoreCase) {
             attrName = attrName.toLowerCase();
         }
         if (sequence==null) {
             getMethods.put(attrName, m);
             seqSet.add(attrName);
         } else if (seqSet.contains(attrName)) {
             getMethods.put(attrName, m);
         }        
    }
    
    private void addSetMethod( String methodName, Method m) {
        if (m.getParameterTypes().length != 1) {
            return;
        }
         String attrName = Introspector.decapitalize(methodName);
         if (ignoreCase) {
             attrName = attrName.toLowerCase();
         }
         if (sequence==null) {
             setMethods.put(attrName, m);
             seqSet.add(attrName);
         } else if (seqSet.contains(attrName)) {
             setMethods.put(attrName, m);
         }        
    }

    Method getGetMethod(String name) {
        if (ignoreCase) {
            name=name.toLowerCase();
        }
        return getMethods.get(name);
    }

    Method getSetMethod(String name) {
        if (ignoreCase) {
            name=name.toLowerCase();
        }
        return setMethods.get(name);
    }

    public String getClassName() {
        return className;
    }
}


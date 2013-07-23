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

import java.util.HashMap;

import com.openlinksw.util.Nameable;

public abstract class IndexedJsonList<T extends Nameable> extends SimpleJsonList<T> {

    HashMap<String, T> index=new HashMap<String, T>(); 

    @Override
    public void add(T element) {
        super.add(element);
        index.put(element.getName(), element);
    }
    
   public T get(String name) {
       return index.get(name);
   }
}

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


public class FrameSchema extends Schema {
    HashMap<String, CellSchema> cells;

    public FrameSchema(String name, CellSchema[] cells) {
        super(name);
        this.cells=new HashMap<String, CellSchema>();
        for (CellSchema cell: cells) {
            this.cells.put(cell.name, cell);
        }
    }

    public FrameSchema(String name, String[][] cellDescriptions) {
        super(name);
        this.cells=new HashMap<String, CellSchema>();
        for (String[] cellDescription: cellDescriptions) {
            CellSchema cell=new CellSchema(cellDescription);
            this.cells.put(cell.name, cell);
        }
    }
    
    public CellSchema getCellSchema(String name) {
        return cells.get(name);
    }
/*
    public void print2sb(StringBuilder sb, HashMap<String, String> keyVals) {
        for (CellSchema cell: cells.values()) {
            String key=cell.getName();
            String val=keyVals.get(key);
            cell.print2sb(sb, val);
        }
    }

    public void values2sb(StringBuilder sb, HashMap<String, String> keyVals, char delim) {
        for (CellSchema cell: cells.values()) {
            String key=cell.getName();
            String val=keyVals.get(key);
            sb.append(val).append(delim);
        }
    }
*/
    public StringBuilder getHeader(char delim) {
        StringBuilder sb=new StringBuilder(); 
        for (CellSchema cell: cells.values()) {
            sb.append(cell.getName()).append(delim);
        }
        return sb;
    }
}


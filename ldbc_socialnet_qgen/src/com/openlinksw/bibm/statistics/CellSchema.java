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


public class CellSchema extends Schema {
    String title;
    String comment;
    
    public CellSchema(String name) {
        super(name);
    }

    public CellSchema(String name, String title, String comment) {
        super(name);
        this.title = title;
        this.comment = comment;
    }

    public CellSchema(String[] cellDescription) {
        super(cellDescription[0]);
        if (cellDescription.length>1) {
            this.title =  cellDescription[1];
        }
        if (cellDescription.length>2) {
            this.comment =  cellDescription[2];
        }
    }

    public void print2sb(StringBuilder sb, String val) {
        sb.append(title!=null?title:name).append(": ");
        sb.append(val);
        if (comment!=null) {
            sb.append(" ").append(comment);
        }
        sb.append("\n");
    }
}


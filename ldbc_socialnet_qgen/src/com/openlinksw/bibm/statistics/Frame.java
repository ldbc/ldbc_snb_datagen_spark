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

import java.util.ArrayList;


/** common machinery for QueryMixStatistics and QueryStatistics
 * 
 * @author ak
 *
 */
public abstract class Frame extends Element {
	protected ArrayList<Element> elems=new ArrayList<Element>();
	
	public Frame(FrameSchema schema) {
		super(schema);
	}

	public void addElem(Element value) {
		elems.add(value);
	}

	public void addElem(Frame elem) {
		elems.add(elem);
	}

	public void addElem(String name, String value) {
		CellSchema sch=((FrameSchema)schema).getCellSchema(name);
		if (sch==null) {
			throw new IllegalArgumentException("no such element: "+name);
		}
		Element elem=new Cell(sch, value);
		elems.add(elem);
	}

	public void addElem(String name, int value) {
		addElem(name, Integer.toString(value));
	}

	public void addElem(String name, double value) {
		addElem(name, Double.toString(value));
	}

	public void addElem(String name, long value) {
		addElem(name, Long.toString(value));
	}

	public String toString() {
		StringBuilder sb=new StringBuilder();
		String name = schema.getName();
		sb.append("\n<").append(name);
		printAttrs(sb);
		sb.append(">");
		for (Element elem: elems) {
			sb.append(elem.toString()).append(" "); // FIXME			
		}
		sb.append("</").append(name).append(">\n");
		return sb.toString();
	}

	/** convert to scv
	 * 
	 * @param delim
	 * @return
	 */
	public StringBuilder getValues(char delim) {
		StringBuilder sb=new StringBuilder(); 
		if (attrs!=null) {
			for (String attr: attrs.values()) {
				sb.append(attr).append(delim);
			}
		}
		for (Element elem: elems) {
			sb.append(elem.toString()).append(delim); // FIXME: NORMALISE
		}
		return sb;
	}

	@Override
	public void print(StringBuilder sb) {
		for (Element elem: elems) {
			if (! (elem instanceof Frame)) {
				elem.print(sb); 			
			}
		}
		for (Element elem: elems) {
			if (elem instanceof Frame) {
				sb.append('\n');
				elem.print(sb); 			
			}
		}
	}
}

class Cell extends Element {
    String value; 
    
	public Cell(CellSchema schema, String value) {
		super(schema);
		this.value = value;
	}

	public String toString() {
		StringBuilder sb=new StringBuilder();
		String name = schema.getName();
		sb.append("<").append(name);
		if (attrs!=null && attrs.size()>0) {
			sb.append(" ");
			for (String attr: attrs.values()) {
				sb.append(attr).append(" ");
			}
		}
		sb.append(">");
		sb.append(value); 			
		sb.append("</").append(name).append(">\n");
		return sb.toString();
	}

	@Override
	public void print(StringBuilder sb) {
		CellSchema schema=(CellSchema)super.schema;
		if (schema.title==null) {
			return; // this cell is not for printing
		}
		sb.append(schema.title).append(value);
		if (schema.comment!=null) {
			sb.append(schema.comment);
		}
		sb.append("\n");
	}

}


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
package com.openlinksw.bibm.sib;

import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.tpch.ParameterPool;

public class SIBFormalParameter extends  FormalParameter{
	public byte parameterType;

	public SIBFormalParameter(byte classByte) {
	    super(true);
		this.parameterType = classByte;
	}

    public  String getDefaultValue() {
	switch (this.parameterType) {
	case SIBParameterPool.PERSON_NAME:
	    return "Meera";
	case SIBParameterPool.PERSON_URI:
	    return "sn:pers2053";
	case SIBParameterPool.PERSON_ID:
	    return "2053";
	case SIBParameterPool.TAG_URI:
	    return "<http://dbpedia.org/resource/George_W._Bush>";
	case SIBParameterPool.TAG_SQL_URI:
	    return "http://dbpedia.org/resource/George_W._Bush";
	case SIBParameterPool.COUNTRY_URI:
	    return "<http://dbpedia.org/resource/India>";
	case SIBParameterPool.COUNTRY_SQL_URI:
	    return "http://dbpedia.org/resource/India";
	case SIBParameterPool.TAG_TYPE_URI:
	    return "<http://dbpedia.org/ontology/Scientist>";
	case SIBParameterPool.TAG_TYPE_SQL_URI:
	    return "http://dbpedia.org/ontology/Scientist";
	}
	throw new UnsupportedOperationException(getClass().getName()+".getValue()");
    }
    
    @Override
    public String getValue(ParameterPool pool) {
        throw new UnsupportedOperationException(getClass().getName()+".getValue()");
    }
    
}

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
package com.openlinksw.bibm.bsbm;

import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.tpch.ParameterPool;

public class BSBMFormalParameter extends  FormalParameter{
	public byte parameterType;

	public BSBMFormalParameter(byte classByte) {
	    super(true);
		this.parameterType = classByte;
	}

    public  String getDefaultValue() {
        throw new UnsupportedOperationException(getClass().getName()+".getDefaultValue()");
    }
    
    @Override
    public String getValue(ParameterPool pool) {
        throw new UnsupportedOperationException(getClass().getName()+".getValue()");
    }
    
}

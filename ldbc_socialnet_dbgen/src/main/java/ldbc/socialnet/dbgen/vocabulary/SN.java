/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
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
package ldbc.socialnet.dbgen.vocabulary;

import java.util.HashMap;

public class SN{
	//The namespace of this vocabulary as String
	public static final String NS = "http://www.ldbc.eu/ldbc_socialnet/1.0/data/";
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
	
	public static int machineId;
		
	public static final String PREFIX = "sn:";
	public static final String FACTPREFIX = "sn_";
	public static final String BLANK_NODE = "_:";
	
	private static HashMap<String, String> uriMap = new HashMap<String, String>();
	
	public static void setMachineNumber(int machineId)
	{
	    SN.machineId = machineId;
	}
	
	public static String prefixed(String string) {
		if(uriMap.containsKey(string)) {
			return uriMap.get(string);
		}
		else {
			String newValue = PREFIX + string;
			uriMap.put(string, newValue);
			return newValue;
		}
	}
	
	public static String factprefixed(String string) {
		if(uriMap.containsKey(string)) {
			return uriMap.get(string);
		}
		else {
			String newValue = FACTPREFIX + string;
			uriMap.put(string, newValue);
			return newValue;
		}
	}
	
	public static String getPersonURI(long id) {
        return PREFIX+"pers"+id;
    }
	
	public static String getForumURI(long id) {
        return PREFIX+"forum"+id + "" + machineId;
    }
	
	public static String getGroupURI(long id) {
        return PREFIX+"group"+id + "" + machineId;
    }
	
	public static String getPostURI(long id) {
        return PREFIX+"post"+id + "" + machineId;
    }
	
	public static String getCommentURI(long id) {
        return PREFIX+"comm"+id + "" + machineId;
    }
	
	public static String getMembershipURI(long id) {
        return BLANK_NODE+"mbs"+id + "" + machineId;
    }
	
	public static String getLikeURI(long id) {
        return BLANK_NODE+"like"+id + "" + machineId;
    }
	
	public static String getSpeaksURI(long id) {
        return BLANK_NODE+"speak"+id + "" + machineId;
    }
	
	public static String getStudyAtURI(long id) {
        return BLANK_NODE+"study"+id + "" + machineId;
    }
	
	public static String getWorkAtURI(long id) {
        return BLANK_NODE+"work"+id + "" + machineId;
    }
	
	public static String formId(long id) {
	    return id + "" + machineId;
	}
}
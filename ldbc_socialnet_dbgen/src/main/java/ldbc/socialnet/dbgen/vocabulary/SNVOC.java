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

public class SNVOC{
	//The namespace of this vocabulary as String
	public static final String NS = "http://www.ldbc.eu/ldbc_socialnet/1.0/data/";
	
	//Get the URI of this vocabulary
	public static String getURI() { return NS; }
		
	public static final String PREFIX = "snvoc:";
	public static final String FACTPREFIX = "snvoc_";
	
	private static HashMap<String, String> uriMap = new HashMap<String, String>();
	
	/*
	 * For prefixed versions
	 */
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
	
	//General usage
	public static final String Created    = PREFIX+"created";
	public static final String Based_near = PREFIX+"based_near";
	public static final String Title      =  PREFIX+"title";
	public static final String Creator_of =  PREFIX+"creator_of";
	public static final String Has_tag    =  PREFIX+"has_tag";
	
	//Location related
	public static final String Location =  PREFIX+"Location";
	public static final String City     =  PREFIX+"City";
	public static final String Country  =  PREFIX+"Country";
	public static final String Region   =  PREFIX+"Region";
	public static final String Name     =  PREFIX+"name";
	public static final String Part_of  =  PREFIX+"part_of";
	
	//Person related
	public static final String Person    =  PREFIX+"Person";
	public static final String FirstName =  PREFIX+"firstName";
	public static final String LastName  =  PREFIX+"lastName";
	public static final String Gender    =  PREFIX+"gender";
	public static final String Birthday  =  PREFIX+"birthday";
	public static final String Has_mail  =  PREFIX+"has_mail";
	public static final String Interest  =  PREFIX+"interest";
	public static final String Knows     =  PREFIX+"knows";
	
	//Connection related
	public static final String Connection  =  PREFIX+"connection";
	public static final String Browser     =  PREFIX+"browser";
	public static final String IPAddress   =  PREFIX+"IPAddress"; //Type
	public static final String Ip_address  =  PREFIX+"ip_address"; //attribute
	public static final String Located_in  =  PREFIX+"located_in";
	
	//Forum related
	public static final String Forum        =  PREFIX+"Forum";
	public static final String Moderator_of =  PREFIX+"moderator_of";
	
	//Group related
	public static final String Group        =  PREFIX+"Group";
	public static final String Membership   =  PREFIX+"membership";
	public static final String Joined       =  PREFIX+"joined";
	
	//Post and Comment related
	public static final String Post          =  PREFIX+"Post";
	public static final String Comment       =  PREFIX+"Comment";
	public static final String Type          =  PREFIX+"type";
	public static final String Content       =  PREFIX+"content";
	public static final String Annotated     =  PREFIX+"annotated";
	public static final String Container_of  =  PREFIX+"container_of";
	public static final String Like          =  PREFIX+"like";
	public static final String Reply_of      =  PREFIX+"reply_of";
	
	//Language related
	public static final String Language      =  PREFIX+"Language";
	public static final String Speaks        =  PREFIX+"speaks";
	public static final String Native        =  PREFIX+"native";
	
	//Organization related
    public static final String Organization  =  PREFIX+"Organization";
    public static final String Study_at      =  PREFIX+"study_at";
    public static final String ClassYear     =  PREFIX+"classYear";
    public static final String Work_at       =  PREFIX+"work_at";
    public static final String WorkFrom      =  PREFIX+"workFrom";
}
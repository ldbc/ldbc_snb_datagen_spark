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

public class XSD {
    public static final String NS = "http://www.w3.org/2001/XMLSchema#";
	public static final String PREFIX = "xsd:";
	public static final String FACTPREFIX = "xsd_";
	
	
	public static String prefixed(String string) {
        return PREFIX + string;
    }

    public static String factprefixed(String string) {
        return  FACTPREFIX + string;
    }
    
    public static String getUrl(String string) {
        return NS + string;
    }

    public static String fullprefixed(String string) {
        return "<" + NS + string + ">";
    }

    public static String getURI() {
        return NS;
    }
	
	//Resources
	public static final String Integer = PREFIX + "integer";
	public static final String Int = PREFIX + "int";
	public static final String Float = PREFIX + "float";
	public static final String Double = PREFIX + "double";
	public static final String Long = PREFIX + "long";
	public static final String String = PREFIX + "string";
	public static final String Decimal = PREFIX + "decimal";
	public static final String Year = PREFIX + "gYear";
	public static final String Date = PREFIX + "date";
	public static final String DateTime = PREFIX + "dateTime";
}
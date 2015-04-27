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
package ldbc.snb.datagen.vocabulary;

/**
 * RDF syntax namespace used in the serialization process.
 */
public class RDF {

    public static final String NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
    public static final String PREFIX = "rdf:";

    //Resources
    public static final String Alt = PREFIX + "Alt";
    public static final String Bag = PREFIX + "Bag";
    public static final String Property = PREFIX + "Property";
    public static final String Seq = PREFIX + "Seq";
    public static final String Statement = PREFIX + "Statement";
    public static final String List = PREFIX + "List";
    public static final String nil = PREFIX + "nil";

    //Properties
    public static final String first = PREFIX + "first";
    public static final String rest = PREFIX + "rest";
    public static final String subject = PREFIX + "subject";
    public static final String predicate = PREFIX + "predicate";
    public static final String object = PREFIX + "object";
    public static final String type = PREFIX + "type";
    public static final String value = PREFIX + "value";

    /**
     * Gets the RDF syntax prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the RDF syntax URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the RDF syntax RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}
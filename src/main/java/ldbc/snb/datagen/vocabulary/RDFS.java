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
 * RDF schema namespace used in the serialization process.
 */
public class RDFS {

    public static final String NAMESPACE = "http://www.w3.org/2000/01/rdf-schema#";
    public static final String PREFIX = "rdfs:";

    //Resources
    public static final String Datatype = PREFIX + "Datatype";
    public static final String Literal = PREFIX + "Literal";
    public static final String Resource = PREFIX + "Resource";

    //Properties
    public static final String comment = PREFIX + "comment";
    public static final String label = PREFIX + "label";
    public static final String subClassOf = PREFIX + "subClassOf";

    /**
     * Gets the RDF schema prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }

    /**
     * Gets the RDF schema URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the RDF schema RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
}

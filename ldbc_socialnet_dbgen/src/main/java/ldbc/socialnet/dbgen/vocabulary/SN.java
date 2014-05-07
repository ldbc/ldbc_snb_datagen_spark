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

/**
 * LDBC social network data namespace used in the serialization process.
 */
public class SN{
    
    public static long machineId;
    private static long numBits;
	public static final String NAMESPACE = "http://www.ldbc.eu/ldbc_socialnet/1.0/data/";
	public static final String PREFIX = "sn:";
	public static final String BLANK_NODE = "_:";
	
	/**
	 * Sets the machine id.
	 * Used as a suffix in some SN entities' tp create unique IDs in parallel generation.
	 */
	public static void setMachineNumber(int machineId, int numMachines) {
/*	    int digits = 0;
	    do {
	        numMachines /= 10;
            digits++;
	    } while (numMachines != 0);
	    SN.machineId = String.valueOf(machineId);
	    for (int i = SN.machineId.length(); i < digits; i++) {
	        SN.machineId = '0' + SN.machineId;
	    }*/
        SN.numBits = (int)Math.ceil(Math.log10(numMachines) / Math.log10(2));
        SN.machineId = machineId;
        if( numBits > 14 ) System.out.print("WARNING: Possible id overlapp");
	}
	
	/**
     * Gets the LDBC social network data prefix version of the input.
     */
    public static String prefixed(String string) {
        return PREFIX + string;
    }
    
    /**
     * Gets the LDBC social network data URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

    /**
     * Gets the LDBC social network data RDF-URL version of the input.
     */
    public static String fullprefixed(String string) {
        return "<" + NAMESPACE + string + ">";
    }
	
    /**
     * Gets the person entity prefix.
     */
	public static String getPersonURI(long id) {
        return PREFIX+"pers"+String.format("%020d", id);
    }
	
	/**
     * Gets the forum entity prefix.
     */
	public static String getForumURI(long id) {
        return PREFIX+"forum"+String.format("%020d",_formId(id));
    }
	
	/**
     * Gets the post entity prefix.
     */
	public static String getPostURI(long id) {
        return PREFIX+"post"+String.format("%020d",_formId(id));
    }
	
	/**
     * Gets the comment entity prefix.
     */
	public static String getCommentURI(long id) {
        return PREFIX+"comm"+String.format("%020d",_formId(id));
    }
	
	/**
     * Gets the membership relation prefix.
     */
	public static String getMembershipURI(long id) {
        return BLANK_NODE+"mbs"+String.format("%020d",_formId(id));
    }
	
	/**
     * Gets the like relation prefix.
     */
	public static String getLikeURI(long id) {
        return BLANK_NODE+"like"+String.format("%020d",_formId(id));
    }
	
	/**
     * Gets the studyAt relation prefix.
     */
	public static String getStudyAtURI(long id) {
        return BLANK_NODE+"study"+String.format("%020d",_formId(id));
    }
	
	/**
     * Gets the workAt relation prefix.
     */
	public static String getWorkAtURI(long id) {
        return BLANK_NODE+"work"+String.format("%020d",_formId(id));
    }

    public static String getKnowsURI(long id) {
        return BLANK_NODE+"knows"+String.format("%020d",_formId(id));
    }

	/**
     * Gets the true id having in consideration the machine.
     */
	public static String formId(long id) {
        return Long.toString(_formId(id));
	}

    public static Long _formId(long id) {
        long lowMask = 0x0FFFFF;                                // This mask is used to get the lowest 20 bits.
        long lowerPart = ( lowMask & id );
        long machinePart = machineId << 20 ;
        long upperPart = (id >> 20) << (20 + numBits);
        return  upperPart | machinePart | lowerPart ;
    }

    public static long composeId( long id, long date ) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 33);
        long dateMask = ~(0xFFFFFFFFFFFFFFFFL << 20);
        return (((date >> 20) & dateMask) << 33) | ((id & idMask)) ;
    }
}
/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.vocabulary;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;

/**
 * LDBC social network data namespace used in the serialization process.
 */
public class SN {

    private static long numBits;
    private static long minDate;
    private static long maxDate;

    /**
     * Sets the machine id.
     * Used as a suffix in some SN entities' tp create unique IDs in parallel generation.
     */
    public static void initialize() {
        minDate = Dictionaries.dates.getSimulationStart();
        maxDate = Dictionaries.dates.getSimulationEnd();
        numBits = (int) Math.ceil(Math.log10(Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize)) / Math.log10(2));
        if (numBits > 20) System.out.print("WARNING: Possible id overlapp");
    }

    public static Long formId(long id, long blockId) {
        long lowMask = 0x0FFFFF;                                // This mask is used to get the lowest 20 bits.
        long lowerPart = (lowMask & id);
        long machinePart = blockId << 20;
        long upperPart = (id >> 20) << (20 + numBits);
        return upperPart | machinePart | lowerPart;
    }

    public static long composeId(long id, long creationDate, long blockId) {
        long bucket = (long) (256 * (creationDate - minDate) / (double) maxDate);
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 36);
        return (bucket << 36) | (id & idMask);
    }

}

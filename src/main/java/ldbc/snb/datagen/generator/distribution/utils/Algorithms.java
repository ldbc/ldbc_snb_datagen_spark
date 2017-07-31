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
package ldbc.snb.datagen.generator.distribution.utils;

import ldbc.snb.datagen.generator.distribution.DegreeDistribution;

/**
 * Created by aprat on 3/02/17.
 */
public class Algorithms {
    private static double scale(long numPersons, double mean) {
        return Math.log10(mean * numPersons / 2 + numPersons);
    }

    public static long findNumPersonsFromGraphalyticsScale(DegreeDistribution distribution, double scale) {

        long numPersonsMin = 1000000;
        while (scale(numPersonsMin, distribution.mean(numPersonsMin)) > scale) {
            numPersonsMin /= 2;
        }

        long numPersonsMax = 1000000;
        while (scale(numPersonsMax, distribution.mean(numPersonsMax)) < scale) {
            numPersonsMax *= 2;
        }

        long currentNumPersons = (numPersonsMax - numPersonsMin) / 2 + numPersonsMin;
        double currentScale = scale(currentNumPersons, distribution.mean(currentNumPersons));
        while (Math.abs(currentScale - scale) / scale > 0.001) {
            if (currentScale < scale) {
                numPersonsMin = currentNumPersons;
            } else {
                numPersonsMax = currentNumPersons;
            }
            currentNumPersons = (numPersonsMax - numPersonsMin) / 2 + numPersonsMin;
            currentScale = scale(currentNumPersons, distribution.mean(currentNumPersons));
            System.out.println(numPersonsMin + " " + numPersonsMax + " " + currentNumPersons + " " + currentScale);
        }
        return currentNumPersons;
    }
}

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
package com.openlinksw.bibm.tpch;

import java.util.Locale;

public abstract class QueryTiming {
    
    /** TPC_H Spec 5.3.7.5:  The timing interval of each query and refresh function ...must be rounded to the nearest 0.1 second.
     * Values of less than 0.05 second must be rounded up to 0.1 second to avoid zero values.
     */
   public abstract  String getTiming();

    protected static String getTiming(double interval) {
        if (interval<0.05d) {
            interval=0.1d;
        }
        return String.format(Locale.US, "%.1f", interval);
    }
}

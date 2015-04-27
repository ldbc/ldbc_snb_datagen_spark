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
package ldbc.snb.datagen.util;


public class ZOrder {

    private int MAX_BIT_NUMBER;

    public ZOrder(int maxNumBit) {
        this.MAX_BIT_NUMBER = maxNumBit;
    }

    public int getZValue(int x, int y) {

        String sX = Integer.toBinaryString(x);
        int numberToAddX = MAX_BIT_NUMBER - sX.length();
        for (int i = 0; i < numberToAddX; i++) {
            sX = "0" + sX;
        }

        String sY = Integer.toBinaryString(y);
        int numberToAddY = MAX_BIT_NUMBER - sY.length();
        for (int i = 0; i < numberToAddY; i++) {
            sY = "0" + sY;
        }

        String sZ = "";
        for (int i = 0; i < sX.length(); i++) {
            sZ = sZ + sX.substring(i, i + 1) + "" + sY.substring(i, i + 1);
        }

        return Integer.parseInt(sZ, 2);
    }
}

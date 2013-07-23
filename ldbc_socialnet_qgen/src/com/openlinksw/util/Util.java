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
package com.openlinksw.util;

import java.util.Arrays;
import java.util.Comparator;

public class Util {

    /**
     * short names precede longer ones,
     * so "1"<"2"<"10<"20"
     */
    public static class NumLexComparator implements Comparator<String> {
        @Override
        public int compare(String n1, String n2) {
            int l1 = n1.length();
            int l2 = n2.length();
            if (l1 == l2) {
                return n1.compareTo(n2);
            } else if (l1 < l2) {
                return -1;
            } else {
                return +1;
            }
        }
    }

    public static final class NumLexNameComparator<T  extends Nameable> implements Comparator<T> {
        NumLexComparator cmp=new NumLexComparator();
        @Override
        public int compare(T o1, T o2) {
            String n1 = o1.getName();
            String n2 = o2.getName();
            return cmp.compare(n1, n2);
        }

    }

    public  static <T extends Nameable> T[] sortNameable(T[] a) {
        Comparator<T> comparat = new NumLexNameComparator<T>();
        Arrays.sort(a, comparat);
        return a;
    }

    /** divides integers with rounding up
     * @param dividend
     * @param divisor
     * @return quotient rounded to upper integer
     */
    public static int divUp(int dividend, int divisor) {
        return (dividend+divisor-1)/divisor;
    }
    
    public static int countLines(String values) {
        int res=0;
        for (int k=0; k<values.length(); k++) {
            if (values.charAt(k)=='\n') {
                res++;
            }
        }
        return res;
    }

}

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
package com.openlinksw.bibm.qualification;

import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.ParserException;
import com.openlinksw.util.json.impl.SimpleJsonObject;

public abstract class ResultDescription extends SimpleJsonObject {
    public static ResultDescription stringResult;
    static {
        SimpleJsonObject descr=new SimpleJsonObject();
        descr.put("type", "str");
        stringResult=new StringResult(descr);
    }
    static String decimalType = "decimal.";
    
    public static ResultDescription newResultDescription(SimpleJsonObject descr) {
        String type= ((String) descr.get("type")).toLowerCase();
        if (type==null || "str".equals(type)) {
            return new StringResult(descr);
        } else if ("real".equals(type)) {
            return new RealResult( descr);
        } else if ("int".equals(type)) {
            return new IntResult(descr);
        } else if ("id".equals(type)) {
            return new IdResult(descr);
        } else if ("date".equals(type)) {
            return new DateResult(descr);
        } else if ("object".equals(type)) {// only for OUT parameters of a function call
            return new ObjectResult(descr);
        } else  if (type.startsWith(decimalType)) {
            return new DecimalResult(descr, type.substring(decimalType.length())); 
        } else {
            throw new ParserException("unknown type:"+type);
        }
    }
    
    /** results which may contain spaces and other problematic characters,
     *   must be enquoted while printinq and dequoted while parsing.
     */
    protected  boolean quoted=false;
    protected Check[] checks;
        
    @SuppressWarnings("unchecked")
    public ResultDescription(SimpleJsonObject descr, boolean quoted) {
        super(descr);
        this.quoted = quoted;
        if (descr == null) {
            checks = new Check[0];
            return;
        }
        Object checksO = descr.get("check");
        if (checksO == null) {
            checks = new Check[0];
            return;
        }
        if (checksO instanceof String) {
            checks = new Check[1];
            checks[0] = makeCheck((String) checksO);
            return;
        } else if (checksO instanceof JsonList) {
            JsonList<String> checksS = (JsonList<String>) checksO;
            checks = new Check[checksS.size()];
            for (int k = 0; k < checks.length; k++) {
                String checkS = checksS.get(k);
                checks[k] = makeCheck(checkS);
            }
        } else {
            throw new BadSetupException(" wrong check type: " + checksO.getClass().getName());
        }

    }
    
    public String canonicalString(String value) {
        return value;
    }

    protected Object nullNumber(Object v1) {
        if (v1 instanceof String) {
            String sv1 = (String) v1;
            if ("null".equalsIgnoreCase(sv1)) {
                v1 = null;
            }
        }
        return v1;
    }

    private Check makeCheck(String checkS) {
        if (checkS.startsWith("$")) {
            long delta=Long.parseLong(checkS.substring(1));
            return new DeltaCheck(delta);
        } else  if (checkS.endsWith("%")) {
            int percent=Integer.parseInt(checkS.substring(0, checkS.length()-1));
            return new PercentCheck(percent);
        } else {
            throw new BadSetupException("uncknown check:"+checkS);
        }
    }

    public String fromUnquoted(Object value) {
        // all values come unquoted
        if (value==null) {
            return "null";
        } else if (quoted) {
            StringBuilder sb=new StringBuilder();
            sb.append(QUOTE).append(value).append(QUOTE);
            return sb.toString(); 
        } else {
            return value.toString();
        }
    }
    
    public String fromQuoted(String value) {
        // all values come quoted
        if (value==null) {
            return "null";
        } else if (quoted) {
        	if( value.startsWith("\'") && value.endsWith("\'") ) {
                return value;
            } else {
            	if( value.startsWith("\"") && value.endsWith("\"") )
            		return "'"+value.substring(1, value.length()-1)+"'";
            	else
            		return "'"+value+"'";
            }
        } else {
        	if( value.startsWith("\"") && value.endsWith("\"") )
        		return value.substring(1, value.length()-1);
        	else
        		return value;
        }
    }

    public abstract boolean compare(Object v1, Object v2);
   
    public Check[] getChecks() {
        return checks;
    }

    static final char QUOTE='\'';
    
    static class StringResult extends ResultDescription {
        public StringResult(SimpleJsonObject descr) {
            super(descr, true);
        }

        public static String trimStringRight(String v)
        {
        	while (v.lastIndexOf(' ') == v.length()-1 )
        		v = v.substring(0, v.length()-1);
        	return v;
        }
        @Override
        public boolean compare(Object v1, Object v2) {
            if (v1==null) {
                return v2==null;
            }
            if (v2==null) {
                return false;
            }
            String s1=((String)v1).trim(); 
            String s2=((String)v2).trim();
            int l1=s1.length();
            if (l1!=s2.length()) {
                return false;
            }
            for (int i = 1; i < l1-1; i++) {
               if (s1.charAt(i)!=s2.charAt(i)) {
                   return false;
               }
            }
            return true;
        }
    }
    
    static class DateResult extends ResultDescription {
        public DateResult(SimpleJsonObject descr) {
            super(descr, true);
        }

        /** ignore time component in Sparql dateTime (e.g. '1995-03-05T00:00:00+07:00')
         */
        @Override
        public String canonicalString(String value) {
            int start=0, end=value.length();
            boolean truncate=false;
            if (value.startsWith("'")||value.startsWith("\"")) {
                start=1; end=end-1;
                truncate=true;
            }
            int posT=value.indexOf('T');
            if (posT>0) {
                end=posT;
                truncate=true;
            }
            if (truncate) {
                return value.substring(start, end);
            } else {
                return value;
            }
        }

        @Override
        public boolean compare(Object v1, Object v2) {
            if (v1==null) {
                return v2==null;
            }
            if (v2==null) {
                return false;
            }
            String[] date1=canonicalString((String)v1).split("-");
            String[] date2=canonicalString((String)v2).split("-");
            int l1=date1.length;
            if (l1!=date2.length) {
                return false;
            }
            try {
                // compare each date component as integers
                for (int i = 0; i < l1; i++) {
                    int n1=Integer.parseInt(date1[i]);
                    int n2=Integer.parseInt(date2[i]);
                    if (n1!=n2) {
                       return false;
                    }
                }
            } catch (NumberFormatException e) {
                return false;
            }
            return true;
        }
    }
    
    static class IntResult extends ResultDescription {
        public IntResult(SimpleJsonObject descr) {
            super(descr, false);
        }

        @Override
        public boolean compare(Object v1, Object v2) {
            v1 = nullNumber(v1);
            v2 = nullNumber(v2);
            if (v1==null) {
                return v2==null;
            }
            if (v2==null) {
                return false;
            }
            long v1l=toLong(v1);
            long v2l=toLong(v2);
            if (checks.length==0) {
                return v1l==v2l;
            }
            for (Check check: checks) {
                if (!check.compare(v1l, v2l)) {
                    return false;
                }
            }
            return true;
        }

        private long toLong(Object v1) {
            if (v1 instanceof Long) {
                return ((Long)v1).longValue();
            } else if (v1 instanceof Integer) {
                return ((Integer)v1).intValue();
            } else if (v1 instanceof String ) {
                return Long.parseLong((String)v1);
            } else {
                throw new IllegalArgumentException("cannot extract long value from "+v1.getClass().getName());
            }
        }
    }
    
    static class IdResult extends IntResult {
        public IdResult(SimpleJsonObject descr) {
            super(descr);
        }

        @Override
        public String fromQuoted(String value) {
            value=super.fromQuoted(value);
            int pos=value.indexOf('_');
            if (pos==-1) {
                return value;
            }
            return value.substring(pos+1);
        }
    }
    
    static class RealResult extends ResultDescription {
        
        RealResult(SimpleJsonObject descr) {
            super(descr, false);
        }
        
        @Override
        public boolean compare(Object v1, Object v2) {
            v1 = nullNumber(v1);
            v2 = nullNumber(v2);
            if (v1==null) {
                return v2==null;
            }
            if (v2==null) {
                return false;
            }
            double v1l=toDouble(v1);
            double v2l=toDouble(v2);
            if (checks.length==0) {
                return v1l==v2l;
            }
            for (Check check: checks) {
                if (!check.compare(v1l, v2l)) {
                    return false;
                }
            }
            return true;
        }

        protected double toDouble(Object v1) {
            if (v1 instanceof Double) {
                return ((Double)v1).doubleValue();
            } else  if (v1 instanceof Float) {
                return ((Float)v1).doubleValue();
            } else  if (v1 instanceof Long) {
                return ((Long)v1).doubleValue();
            } else if (v1 instanceof Integer) {
                return ((Integer)v1).doubleValue();
            } else if (v1 instanceof String ) {
                return Double.parseDouble((String)v1);
            } else {
                throw new IllegalArgumentException("cannot extract double value from "+v1.getClass().getName());
            }
        }

        @Override
        public String canonicalString(String value) {
            throw new UnsupportedOperationException("RealResult.canonicalString()");
        }
    }

    static class DecimalResult extends RealResult {
        int factor; // 10^precision 
        int precision;
        
        /**
         * 
         * @param descr
         * @param precision number of meaningful digits after point
         */
        DecimalResult(SimpleJsonObject descr, String precision) {
            super(descr);
            try {
                this.precision=Integer.parseInt(precision);
                factor=1;
                for (int k=0; k<this.precision; k++) {
                    factor=factor*10;
                }
            } catch (NumberFormatException e) {
                throw new ParserException("bad decimal precision");
            }
        }

        @Override
        public boolean compare(Object v1, Object v2) {
            if (v1==null) {
                return v2==null;
            }
            if (v2==null) {
                return false;
            }
            double v1d=toDouble(v1);
            double v2d=toDouble(v2);
            if (checks.length==0) {
                long v1l=Math.round(v1d*factor); 
                long v2l=Math.round(v2d*factor); 
                return v1l==v2l;
            }
            for (Check check: checks) {
                if (!check.compare(v1d, v2d)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String canonicalString(String value) {
            int actualPrecision = value.length() - 1 - value.indexOf('.');
            int delta = precision - actualPrecision;
            if (delta == 0) {
                return value;
            } else if (delta > 0) {
                StringBuilder sb = new StringBuilder(value);
                for (int k = 0; k < delta; k++) {
                    sb.append('0');
                }
                return sb.toString();
            } else {  // delta<0
                int canonicalLength = value.length() + delta;
                for (int k = canonicalLength; k < value.length(); k++) {
                    if (value.charAt(k) != '0') {
                        throw new NumberFormatException(value + ": only " + precision + " digits allowed");
                    }
                }
                return value.substring(0, canonicalLength);
            }
        }

   }

    static class ObjectResult extends ResultDescription {
        public ObjectResult(SimpleJsonObject descr) {
            super(descr, true);
        }

        @Override
        public boolean compare(Object v1, Object v2) {
            throw new UnsupportedOperationException("ObjectResult.compare()");
        }
    }

    public static abstract class Check {

        public abstract boolean compare(long v1, long v2);
        public abstract boolean compare(double v1, double v2);

    }

    public static class DeltaCheck extends Check {
        long delta;
        
        public DeltaCheck(long delta) {
            this.delta = delta;
        }

        public boolean compare(long v1, long v2) {
            return compare((double)v1, (double)v2);
        }

        public boolean compare(double v1, double v2) {
            return Math.abs(v1-v2)<=delta;
        }

    }

    /** 0.99*v<=round(r,2)<=1.01*v.
     */
    public class PercentCheck extends Check {
        double lo, hi;

        public PercentCheck(int percent) {
            this.lo = 1.0d-percent/100d;
            this.hi = 1.0d+percent/100d;
        }

        @Override
        public boolean compare(long v1, long v2) {
            return compare((double)v1, (double)v2);
        }

        public boolean compare(double validValue, double checkValue) {
            validValue=Math.round(validValue*100); // enable compare non-etalon results
            long rounded=Math.round(checkValue*100);
            return lo*validValue<=rounded && rounded <=hi*validValue;
        }
    }
    
}

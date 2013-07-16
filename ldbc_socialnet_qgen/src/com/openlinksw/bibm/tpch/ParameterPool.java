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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import com.openlinksw.bibm.AbstractParameterPool;
import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;


public class ParameterPool extends AbstractParameterPool {
	Random numgen;
	 double scaleFactor;
	 
	public ParameterPool(char parameterChar, long seed, double scaleFactor) {
		super.parameterChar=parameterChar;
		this.scaleFactor= scaleFactor;
		numgen= new Random(seed);
	}

	@Override
	public  FormalParameter createFormalParameter(String paramClass, String[] addPI, String defaultValue) {
		Class<? extends FormalParameter> type = parameterMapping.get(paramClass);
		if (type == null) {
			throw new BadSetupException("Unknown parameter class: " + paramClass);
		}
		FormalParameter parameter;
		try {
			parameter = type.newInstance();
		} catch (Exception e) {
			throw new ExceptionException("cannot instantiate "+type.getName(), e);
		}
        parameter.setDefaultValue(defaultValue);
		parameter.init(addPI);
		return parameter;
	}
	
	@Override
	public String[] getParametersForQuery(Query query, int level) {
		FormalParameter[] fps=query.getFormalParameters();
		int paramCount=fps.length;
		String[] parameters = new String[paramCount];
		
		for (int i=0; i<paramCount; i++) {
			FormalParameter fp=fps[i];
			parameters[i] = fp.getValue(this);
		}
		return parameters;
	}


	String getRandomElement(String[] array) {
		return array[numgen.nextInt(array.length)];
	}

	@Override
	public double getScalefactor() {
		return  scaleFactor;
	}
	

    /* FormalParameter  ========================================*/

	/** real value = value/factor
	 */
	static class FixedFloat {
		int fracWidth;
		int factor=1;
		int value;
		
		public FixedFloat(String strVal) {
			int pointPos=strVal.indexOf('.');
			if (pointPos==-1) {
				fracWidth=0;
				value=Integer.valueOf(strVal);
			} else {
// 				factor=Math.pow(10, strVal.length()-pointPos);
				fracWidth = strVal.length()-(pointPos+1);
				for (int k=0; k<fracWidth; k++) {
					factor*=10;
				}
				value=Integer.valueOf(strVal.substring(0, pointPos)) * factor +Integer.valueOf(strVal.substring(pointPos+1));
			}	
		}
		
	}

	protected static class RandomFP extends FormalParameter {
		private int fracWidth;
		private int factor;
		private int lo;
		private int interval;

		@Override
		public void init(String[] args) {
            if (args.length!=2) {
                throw new BadSetupException("rdfh.Random: bad number of subparameters: "+args.length);
            }
			FixedFloat low=new FixedFloat(args[0]);
			FixedFloat hi=new FixedFloat(args[1]);
			if (low.fracWidth!=hi.fracWidth) {
				throw new BadSetupException("rdfh.Random: bad interval: "+args[0]+".."+args[1]);
			}
			fracWidth=low.fracWidth;
			factor=low.factor;
			lo=low.value;
			interval=hi.value-low.value;
			if (interval<0) {
				throw new BadSetupException("rdfh.Random: bad interval: "+args[0]+".."+args[1]);
			}
		}
		
		@Override
		public String getValue(ParameterPool pool) {
			int resValue=lo+pool.numgen.nextInt(interval+1);
			if (fracWidth==0) {
				return Integer.toString(resValue);
			}
			int intPart=resValue/factor;
			int fracPart=resValue%factor;
			String format="%d.%0"+fracWidth+"d";
			return String.format(Locale.US, format, intPart, fracPart);
		}
		
	}

	/* Types ========================================*/
	
	protected  static class TypesFP extends FormalParameter {
		static final String[][] Types={
			{"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"},
			{"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"},
			{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}
		};
		
		private int[] syllabs=new int[3];

		public TypesFP() {
            super(true);
        }

        @Override
		public void init(String[] args) {
			for (int k=0; k<args.length; k++) {
	            String nums=args[k];
				int num = Integer.parseInt(nums);
				syllabs[k]=num;
				if (num<1 || num>3) {
					throw new BadSetupException("Types: bad Type: "+nums);
				}
			}
		}
		
		@Override
		public String getValue(ParameterPool pool) {
			StringBuilder sb=new StringBuilder();
			boolean first=true;
			for (int k=0; k<syllabs.length; k++) {
				int colInd=syllabs[k];
				if (colInd==0) break;
				if (first) {
					first=false;
				} else {
					sb.append(' ');
				}
				sb.append(pool.getRandomElement(Types[colInd-1]));
			}
			return sb.toString();
		}

 	}

	/* Nations ========================================*/
	/* some parameters are interdependent ================*/

    int lastNationIndex;
	
	static final String[] Nations={"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA",
		"FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA",
		"MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM",
		"RUSSIA", "UNITED KINGDOM", "UNITED STATES"
	};
	
	protected  static class NationFP extends FormalParameter {
		
        public NationFP() {
            super(true);
        }

        @Override
		public String getValue(ParameterPool pool) {
			int  nationIndex = pool.numgen.nextInt(Nations.length);
			pool.lastNationIndex=nationIndex;
			return Nations[nationIndex];
		}
		
	}

	protected  static class Nation2FP extends FormalParameter {
		
		public Nation2FP() {
            super(true);
        }

        @Override
		public String getValue(ParameterPool pool) {
			int  nationIndex1=pool.lastNationIndex;
			int  nationIndex2 = pool.numgen.nextInt(Nations.length-1);
			if (nationIndex2>=nationIndex1) {
				nationIndex2++;
			}
			return Nations[nationIndex2];
		}
		
	}
	
	protected  static class CountryCodeSetFP extends FormalParameter {
		int setCount;
		
        @Override
        public String getDefaultValue() {
            // TODO Auto-generated method stub
            return super.getDefaultValue();
        }

        @Override
        public void setDefaultValue(String defaultValue) {
            // TODO Auto-generated method stub
            super.setDefaultValue(defaultValue);
        }

        public CountryCodeSetFP() {
            super(false);
        }

		@Override
		public void init(String[] args) {
		    String counter=args[0];
			setCount = Integer.parseInt(counter);
		}

		@Override
		public String getValue(ParameterPool pool) {
			ArrayList<Integer> countries=new ArrayList<Integer>();
			for (int k=0; k<Nations.length; k++) {
				countries.add(k);
			}
			StringBuilder sb=new StringBuilder();
			boolean first=true;
			for (int k=0; k<setCount; k++) {
				int countryIdx=pool.numgen.nextInt(countries.size());
				Integer country=countries.get(countryIdx);
				countries.remove(countryIdx);
				if (first) {
					first=false;
				} else {
					sb.append(", ");
				}
				sb.append('\'').append(10+country.intValue()).append('\'');
			}
			return sb.toString();
		}

		/**
		 * the whole method toString(Object param) was introduced for the FormalParameter class,
		 * as it is an exception: it returns a string which contains quote characters.
		 * Also, space characters must be deleted, to produce correct key 
		 */
		@Override
        public String toString(Object param) {
            String[] params = ((String)param).split(("\\s+"));
            StringBuilder sb=new StringBuilder();
            for (String p: params) {
                sb.append(p);
            }
            return sb.toString();
        }
	        
	}

	/* Regions ========================================*/
	
	static final String[] Regions={"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
	static final int[] NationRegions={0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};
	
	protected  static class RegionFP extends FormalParameter {

        public RegionFP() {
            super(true);
        }

		@Override
		public String getValue(ParameterPool pool) {
			return pool.getRandomElement(Regions);
		}
        
	}
	
	protected  static class RegionForNationFP extends FormalParameter {

        public RegionForNationFP() {
            super(true);
        }

		@Override
		public String getValue(ParameterPool pool) {
			int  nationIndex1=pool.lastNationIndex;
			return Regions[NationRegions[nationIndex1]];
		}
        
	}

	/* Dates ========================================*/
	protected  static class DateFP extends FormalParameter {
		static final int millisInDay=24*60*60*1000;
		static TimeZone tz=TimeZone.getTimeZone("GMT+0");
		long lo;
		long hi;
		
		@Override
		public void init(String[] args) {
			if (args.length!=2) {
				throw new BadSetupException("rdfh.Random: bad number of subparameters:"+args.length);
			}
			lo=date2days(args[0]);
			hi=date2days(args[1]);
			if (lo>hi) {
				throw new BadSetupException("rdfh.Random: bad date interval:"+args[0]+".."+args[1]);
			}
		}
		
		@Override
		public String getValue(ParameterPool pool) {
			int interval = (int)(hi-lo+1);
			if (interval<=0) {
				throw new BadSetupException("rdfh.Random: bad date interval:"+lo+".."+hi+"="+interval);
			}
			long day=lo+pool.numgen.nextInt(interval);
			return days2date(day);
		}

		static long date2days(String arg) {
			String[] parts=arg.split("-");
			int years=Integer.parseInt(parts[0]);
			int month=0;
			if (parts.length>1) {
				month=Integer.parseInt(parts[1])-1;
			}
			int days=1;
			if (parts.length>2) {
				days=Integer.parseInt(parts[2]);
			}
			Calendar cal=Calendar.getInstance();
			cal.setTimeZone(tz);
			cal.set(years, month, days);
			return cal.getTime().getTime()/millisInDay;
		}

		static String days2date(long day) {
			Date date=new Date(day*millisInDay);
			Calendar cal=Calendar.getInstance();
			cal.setTimeZone(tz);
			cal.setTime(date);
			return String.format("%04d-%02d-%02d", cal.get(Calendar.YEAR),  cal.get(Calendar.MONTH)+1,  cal.get(Calendar.DATE));			
		}
  }

	/* Months ========================================*/
	
	protected  static class MonthFP extends FormalParameter {
		int lo, hi;
		
		@Override
		public void init(String[] args) {
			if (args.length!=2) {
				throw new BadSetupException("rdfh.Random: bad number of subparameters:"+args.length);
			}
			lo=monthDate2num(args[0]);
			hi=monthDate2num(args[1]);
			if (lo>hi) {
				throw new BadSetupException("rdfh.Random: bad date interval:"+args[0]+".."+args[1]);
			}
		}

		@Override
		public String getValue(ParameterPool pool) {
			int interval = hi-lo+1;
			int day=lo+pool.numgen.nextInt(interval);
			return num2monthDate(day);
		}
		
		static int monthDate2num(String arg) {
			String[] parts=arg.split("-");
			int years=Integer.parseInt(parts[0]);
			int month=0;
			if (parts.length>1) {
				month=Integer.parseInt(parts[1])-1;
			}
			return years*12+month;
		}

		static String num2monthDate(int num) {
			int year=num/12;
			int month=num%12;
			return String.format("%04d-%02d", year,  month+1);			
		}
	}

	//P_NAME generated by concatenating five unique randomly selected strings from the following list, separated by a single space:
	
	/* Segments ========================================*/
	
	static final 	String[] Colors={"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood",
		"burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", 
		"drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian",
		"ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "mid­night",
		"mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale",	"papaya", "peach", "peru", "pink", "plum", "powder",
		"puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy",	"seashell", "sienna", "sky", "slate", "smoke", "snow",
		"spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"
   };
	
	protected  static class ColorFP extends FormalParameter {

		@Override
		public String getValue(ParameterPool pool) {
			return  pool.getRandomElement(Colors);
		}
		
	}

	/* Fraction ========================================*/
	
	protected  static class FractionFP extends FormalParameter {

		@Override
		public String getValue(ParameterPool pool) {
			double  fraction=0.0001d/pool.getScalefactor();
			return String.format(Locale.US, "%f", fraction);
		}
		
	}

	/* Ship Modes ========================================*/
	
	static final String[] Modes={
		"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"
	};

    int lastModeIndex;  

	protected  static class ShipmodeFP extends FormalParameter {
		
        public ShipmodeFP() {
            super(true);
        }

        @Override
		public String getValue(ParameterPool pool) {
			int  shmIndex = pool.numgen.nextInt(Modes.length);
			pool.lastModeIndex=shmIndex;
			return Modes[shmIndex];
		}
		
	}

	protected  static class Shipmode2FP extends FormalParameter {
		
        public Shipmode2FP() {
            super(true);
        }

        @Override
		public String getValue(ParameterPool pool) {
			int  shmIndex1=pool.lastModeIndex;
			int  shmIndex2 = pool.numgen.nextInt(Modes.length-1);
			if (shmIndex2>=shmIndex1) {
				shmIndex2++;
			}
			return Modes[shmIndex2];
		}
		
	}

	/* One Of ========================================*/
	
	protected  static class OneOfFP extends FormalParameter {
		String[] variants;
		
		@Override
		public void init(String[] addPI) {
			variants=addPI;
		}
		
		@Override
		public String getValue(ParameterPool pool) {
			return pool.getRandomElement(variants);
		}
		
	}

	/* Brand ========================================*/
	
	protected  static class BrandFP extends FormalParameter {
		
        /**  must be quoted as contains character '#' which is json comment
         */
        public BrandFP() {
            super(true);
        }

		@Override
		public String getValue(ParameterPool pool) {
			String M=Integer.toString(1+pool.numgen.nextInt(5));
			String N=Integer.toString(1+pool.numgen.nextInt(5));
			return "BRAND#"+M+N;
		}
		
	}

	/* Containers ========================================*/
    protected  static class ContainerFP extends FormalParameter {
        static final String[][] Containers={
            {"SM", "LG", "MED", "JUMBO", "WRAP"},
            {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"},
         };
        
        public ContainerFP() {
            super(true);
        }

        @Override
        public String getValue(ParameterPool pool) {
            return
            Containers[0][pool.numgen.nextInt(Containers[0].length)]+' '+
            Containers[1][pool.numgen.nextInt(Containers[1].length)];
        }
        
    }

	/* Initialize Parameter mappings =======================*/
	private static Map<String, Class<? extends FormalParameter>> parameterMapping;
	static {
		parameterMapping = new HashMap<String, Class<? extends FormalParameter>>();
        parameterMapping.put("Brand",   BrandFP.class);
        parameterMapping.put("Date",    DateFP.class);
        parameterMapping.put("Color",   ColorFP.class);
        parameterMapping.put("Container",   ContainerFP.class);
        parameterMapping.put("CountryCodeSet",  CountryCodeSetFP.class);
        parameterMapping.put("Fraction",    FractionFP.class);
        parameterMapping.put("OneOf",   OneOfFP.class);
        parameterMapping.put("Month",   MonthFP.class);
		parameterMapping.put("Nation",	NationFP.class);
		parameterMapping.put("Nation2",	Nation2FP.class);
        parameterMapping.put("Random",  RandomFP.class);
		parameterMapping.put("Region",	RegionFP.class);
		parameterMapping.put("RegionForNation",	RegionForNationFP.class);
		parameterMapping.put("Shipmode",	ShipmodeFP.class);
		parameterMapping.put("Shipmode2",	Shipmode2FP.class);
        parameterMapping.put("Type",    TypesFP.class);
	}
}

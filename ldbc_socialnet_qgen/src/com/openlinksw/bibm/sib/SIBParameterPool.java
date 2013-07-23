package com.openlinksw.bibm.sib;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.openlinksw.bibm.AbstractParameterPool;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.util.DoubleLogger;

import benchmark.generator.Generator;
import benchmark.generator.RandomBucket;
import benchmark.generator.ValueGenerator;
import benchmark.model.ProductType;
import benchmark.tools.NGram;
import benchmark.tools.Operator;
import benchmark.tools.Selectivity;

public abstract class SIBParameterPool extends AbstractParameterPool {
    static final int baseScaleFactor = 284826; // ~100 000 triples is the
                                               // standard database size

    // Parameter constants    
    protected static final byte PERSON_NAME = 101;
    protected static final byte PERSON_URI = 102;
    protected static final byte CREATION_POST_DATE = 103;
    protected static final byte TOTAL_DAYS_OF_CREATION_POST = 104;
    protected static final byte COUNTRY_PAIR = 105;
    protected static final byte TAG_URI = 106;
    protected static final byte COUNTRY_URI = 107;
    protected static final byte HOROSCOPE_SIGN = 108;
    protected static final byte WORK_FROM_DATE = 109;
    protected static final byte TAG_TYPE_URI = 110;
    protected static final byte BIG_COUNTRY_URI = 111;
    protected static final byte TAG_AND_NAME = 112;
    protected static final byte FAMOUS_PERSON = 113;

    // Initialize Parameter mappings
    private static Map<String, Byte> parameterMapping;
    static {
        parameterMapping = new HashMap<String, Byte>();
        parameterMapping.put("PersonName", PERSON_NAME);
        parameterMapping.put("PersonURI", PERSON_URI);
        parameterMapping.put("CreationPostDate", CREATION_POST_DATE);
        parameterMapping.put("TotalDaysOfCreationPost", TOTAL_DAYS_OF_CREATION_POST);
        parameterMapping.put("CountryPair", COUNTRY_PAIR);
        parameterMapping.put("TagURI", TAG_URI);
        parameterMapping.put("CountryURI", COUNTRY_URI);
        parameterMapping.put("HoroscopeSign", HOROSCOPE_SIGN);
        parameterMapping.put("WorkFromDate", WORK_FROM_DATE);
        parameterMapping.put("TagTypeURI", TAG_TYPE_URI);
        parameterMapping.put("BigCountryURI", BIG_COUNTRY_URI);
        parameterMapping.put("TagAndName", TAG_AND_NAME);
        parameterMapping.put("FamousPerson", FAMOUS_PERSON);
    }

    protected ValueGenerator valueGen;
    protected GregorianCalendar currentDate;
    protected Random seedGen;
    
    protected String[] nameList;
    protected int maxNumberOfPerson;
    protected GregorianCalendar creationPostDateStart;
    protected GregorianCalendar creationPostDateEnd;
    protected int totalDaysOfCreationPost;
    protected String[] countryPairs;
    protected String[] tagList;
    protected String[] countryList;
    protected GregorianCalendar workFromDateStart;
    protected GregorianCalendar workFromDateEnd;
    protected int totalDaysOfWorkFrom;
    protected String[] tagTypeList;
    protected String[] bigCountryList;
    protected String[] tagAndNameList;
    protected String[] famousPersonList;

    public double getScalefactor() {
        return 1;
    }
    
    protected void init(File resourceDir, long seed) {      
        	seedGen = new Random(seed);
        	valueGen = new ValueGenerator(seedGen.nextLong());
    	
        	readNames(resourceDir);
        	readPersons(resourceDir);
        	readCreationPostDates(resourceDir);
        	readCountryPairs(resourceDir);
        	readTags(resourceDir);
        	readCountries(resourceDir);
        	readWorkFromDates(resourceDir);
        	readTagTypes(resourceDir);
        	readBigCountries(resourceDir);
        	readTagAndName(resourceDir);
        	readFamousPersons(resourceDir);
    }
    	
    private void readNames(File resourceDir) {    	
        File file = new File(resourceDir, "givennameByCountryBirthPlace.txt.freq.full");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> names = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			String[] parts = line.split("  ");
    			names.add(parts[1]);
    		}
    		reader.close();
    		nameList = names.toArray(new String[names.size()]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readPersons(File resourceDir) {    	
        File file = new File(resourceDir, "personNumber.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = reader.readLine();
    		maxNumberOfPerson = Integer.parseInt(line);
    		reader.close();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readCreationPostDates(File resourceDir) {    	
        File file = new File(resourceDir, "creationPostDate.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = reader.readLine();
    		String[] parts = line.split("-");
    		creationPostDateStart = new GregorianCalendar(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    		line = reader.readLine();
    		parts = line.split("-");
    		creationPostDateEnd = new GregorianCalendar(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    		totalDaysOfCreationPost = (int)((creationPostDateEnd.getTime().getTime() - creationPostDateStart.getTime().getTime()) / (1000 * 60 * 60 * 24));
    		reader.close();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }
    
    private void readCountryPairs(File resourceDir) {    	
        File file = new File(resourceDir, "countryPairs.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> countries = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			String[] parts = line.split(" ");
    			countries.add(parts[0]);
    			countries.add(parts[1]);
    		}
    		reader.close();
    		countryPairs = countries.toArray(new String[countries.size()]);
    		reader.close();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readTags(File resourceDir) {    	
        File file = new File(resourceDir, "dicTopic.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> tags = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			int j = line.indexOf(' ');
    			tags.add(line.substring(j + 1));
    		}
    		reader.close();
    		tagList = tags.toArray(new String[tags.size()]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readCountries(File resourceDir) {    	
        File file = new File(resourceDir, "countries.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> countries = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			countries.add(line);
    		}
    		reader.close();
    		countryList = countries.toArray(new String[countries.size()]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }
    
    private void readWorkFromDates(File resourceDir) {    	
        File file = new File(resourceDir, "workFromDate.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = reader.readLine();
    		String[] parts = line.split("-");
    		workFromDateStart = new GregorianCalendar(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    		line = reader.readLine();
    		parts = line.split("-");
    		workFromDateEnd = new GregorianCalendar(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    		totalDaysOfWorkFrom = (int)((workFromDateEnd.getTime().getTime() - workFromDateStart.getTime().getTime()) / (1000 * 60 * 60 * 24));
    		reader.close();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readTagTypes(File resourceDir) {    	
        File file = new File(resourceDir, "tagTypes.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> tagTypes = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			String[] parts = line.split(" ");
    			tagTypes.add(parts[0]);
    		}
    		reader.close();
    		tagTypeList = tagTypes.toArray(new String[tagTypes.size()]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readBigCountries(File resourceDir) {    	
        File file = new File(resourceDir, "bigCountries.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> countries = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			countries.add(line);
    		}
    		reader.close();
    		bigCountryList = countries.toArray(new String[countries.size()]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }
    
    private void readTagAndName(File resourceDir) {    	
        File file = new File(resourceDir, "tagAndName.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> tagAndNames = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			String[] parts = line.split(" ");
    			tagAndNames.add(parts[0]);
    			tagAndNames.add(parts[1]);
    		}
    		reader.close();
    		tagAndNameList = tagAndNames.toArray(new String[tagAndNames.size()]);
    		reader.close();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }

    private void readFamousPersons(File resourceDir) {    	
        File file = new File(resourceDir, "famousPersons.txt");
        try {
    		BufferedReader reader = new BufferedReader(new FileReader(file));
    		String line = null;
    		ArrayList<String> names = new ArrayList<String>();
    		while ((line = reader.readLine()) != null) {
    			String[] parts = line.split(" ");
    			names.add(parts[0]);
    		}
    		reader.close();
    		famousPersonList = names.toArray(new String[names.size()]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + file.getAbsolutePath(), e);
        }
    }
    
    public SIBFormalParameter createFormalParameter(String paramClass, String[] addPI, String defaultValue) {
        Byte byteType = parameterMapping.get(paramClass);
        if (byteType == null) {
            throw new BadSetupException("Unknown parameter class: " + paramClass);
        }
        switch (byteType) {
        case CREATION_POST_DATE:
        	return new CreationPostDateFP(this, addPI);
        case TOTAL_DAYS_OF_CREATION_POST:
        	return new TotalDaysOfCreationPostFP(this, addPI);
        case COUNTRY_PAIR:
        	return new CountryPairFP(this, addPI);
        case HOROSCOPE_SIGN:
        	return new HoroscopeFP(this, addPI);
        case WORK_FROM_DATE:
        	return new WorkFromDateFP(this, addPI);
        case TAG_AND_NAME:
        	return new TagAndNameFP(this, addPI);
        default:
            return new SIBFormalParameter(byteType);
        }
    }

    /**
     * Format the date string DBMS dependent
     * 
     * @param date
     *            The object to transform into a string representation
     * @return formatted String
     */
    abstract protected String formatDateString(GregorianCalendar date);

    /*
     * Returns a random number between 1-500
     */
    protected Integer getProductPropertyNumeric() {
        return valueGen.randomInt(1, 500);
    }

    static class CreationPostDateFP extends SIBFormalParameter {
        SIBParameterPool parameterPool;
        int percentStart, percentEnd;

        public CreationPostDateFP(SIBParameterPool parameterPool, String[] addPI) {
            super(SIBParameterPool.CREATION_POST_DATE);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 2)
                    throw new IllegalArgumentException("Illegal parameters numbers for CreationPostDate: "+addPI.length+" (expected:2)");
                percentStart = Integer.parseInt(addPI[0]);
                percentEnd = Integer.parseInt(addPI[1]);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for CreationPostDate: " + addPI);
            }
        }

        public Object getRandomCreationPostDate() {
        	int start = percentStart * parameterPool.totalDaysOfCreationPost / 100;
        	int end = percentEnd * parameterPool.totalDaysOfCreationPost / 100;
    		Integer i = parameterPool.valueGen.randomInt(start, end);
    		
    		GregorianCalendar g = (GregorianCalendar) parameterPool.creationPostDateStart.clone();
    		Date d = g.getTime();
    		g.add(GregorianCalendar.DAY_OF_MONTH, i);
    		d = g.getTime();
    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    		return sdf.format(g.getTime());
        }
    }

    static class TotalDaysOfCreationPostFP extends SIBFormalParameter {
        SIBParameterPool parameterPool;
        int percentStart, percentEnd;

        public TotalDaysOfCreationPostFP(SIBParameterPool parameterPool, String[] addPI) {
            super(SIBParameterPool.TOTAL_DAYS_OF_CREATION_POST);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 2)
                    throw new IllegalArgumentException("Illegal parameters numbers for TotalDaysOfCreationPost: "+addPI.length+" (expected:2)");
                percentStart = Integer.parseInt(addPI[0]);
                percentEnd = Integer.parseInt(addPI[1]);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for TotalDaysOfCreationPost: " + addPI);
            }
        }

        public Object getPercenOfTotalDaysCreationPost() {
        	double percent = parameterPool.totalDaysOfCreationPost / 100.0;
        	return parameterPool.valueGen.randomInt((int)(percentStart * percent), (int)(percentEnd * percent));
        }
    }
    
    static class CountryPairFP extends SIBFormalParameter {
        SIBParameterPool parameterPool;
        boolean first;

        public CountryPairFP(SIBParameterPool parameterPool, String[] addPI) {
            super(SIBParameterPool.COUNTRY_PAIR);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 1)
                    throw new IllegalArgumentException("Illegal parameters numbers for CountryPair: "+addPI.length+" (expected:1)");
                first = (addPI[0].equals("1") ? true : false);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for CountryPair: " + addPI);
            }
        }

        public Object getCountry(int index) {
	    return "dbpedia:" + (first ? parameterPool.countryPairs[2 * index] : parameterPool.countryPairs[2 * index + 1]);
        }
    }
    
    static class HoroscopeFP extends SIBFormalParameter {
        SIBParameterPool parameterPool;
        int nth;

        public HoroscopeFP(SIBParameterPool parameterPool, String[] addPI) {
            super(SIBParameterPool.HOROSCOPE_SIGN);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 1)
                    throw new IllegalArgumentException("Illegal parameters numbers for Horoscope: "+addPI.length+" (expected:1)");
                nth = Integer.parseInt(addPI[0]);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for Horoscope: " + addPI);
            }
        }

        public Object getMonth(int index) {
        	int tmp = index + nth;
        	if (tmp > 12)
        		tmp = 1;
        	return tmp;
        }
    }
    
    static class WorkFromDateFP extends SIBFormalParameter {
        SIBParameterPool parameterPool;
        int percentStart, percentEnd;

        public WorkFromDateFP(SIBParameterPool parameterPool, String[] addPI) {
            super(SIBParameterPool.WORK_FROM_DATE);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 2)
                    throw new IllegalArgumentException("Illegal parameters numbers for WorkFromDate: "+addPI.length+" (expected:2)");
                percentStart = Integer.parseInt(addPI[0]);
                percentEnd = Integer.parseInt(addPI[1]);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for WorkFromDate: " + addPI);
            }
        }

        public Object getRandomWorkFromDate() {
        	int start = percentStart * parameterPool.totalDaysOfWorkFrom / 100;
        	int end = percentEnd * parameterPool.totalDaysOfWorkFrom / 100;
    		Integer i = parameterPool.valueGen.randomInt(start, end);
    		
    		GregorianCalendar g = (GregorianCalendar) parameterPool.workFromDateStart.clone();
    		g.add(GregorianCalendar.DAY_OF_MONTH, i);
    		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    		return sdf.format(g.getTime());
        }
    }
    
    static class TagAndNameFP extends SIBFormalParameter {
        SIBParameterPool parameterPool;
        boolean first;

        public TagAndNameFP(SIBParameterPool parameterPool, String[] addPI) {
            super(SIBParameterPool.TAG_AND_NAME);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 1)
                    throw new IllegalArgumentException("Illegal parameters numbers for CountryPair: "+addPI.length+" (expected:1)");
                first = (addPI[0].equals("tag") ? true : false);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for CountryPair: " + addPI);
            }
        }

        public Object getTagOrName(int index) {
        	return first ? parameterPool.tagAndNameList[2 * index] : "dbpedia:" + parameterPool.tagAndNameList[2 * index + 1];
        }
    }

}

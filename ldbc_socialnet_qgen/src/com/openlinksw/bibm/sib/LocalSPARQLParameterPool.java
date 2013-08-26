package com.openlinksw.bibm.sib;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import benchmark.generator.DateGenerator;
import benchmark.generator.RandomBucket;
import benchmark.model.Offer;
import benchmark.model.Producer;
import benchmark.model.Product;
import benchmark.model.ProductFeature;
import benchmark.model.ProductType;
import benchmark.model.Review;
import benchmark.vocabulary.ISO3166;
import benchmark.vocabulary.XSD;

import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.bibm.Exceptions.ExceptionException;
import com.openlinksw.util.DoubleLogger;

public class LocalSPARQLParameterPool extends SIBParameterPool {
	private File updateDatasetFile;
	private BufferedReader updateFileReader = null;
	private GregorianCalendar publishDateMin = new GregorianCalendar(2007,5,20);
	
	public LocalSPARQLParameterPool(File resourceDirectory, Long seed) {
		parameterChar='%';
		init(resourceDirectory, seed);
	}
	
	public LocalSPARQLParameterPool(File resourceDirectory, long seed, File updateDatasetFile) {
		this(resourceDirectory, seed);
		this.updateDatasetFile=updateDatasetFile;
		try {
			updateFileReader = new BufferedReader(new FileReader(updateDatasetFile));
		} catch(IOException e) {
			throw new ExceptionException("Could not open update dataset file: " + updateDatasetFile.getAbsolutePath(), e);
		}
	}
	
	@Override
	public Object[] getParametersForQuery(Query query, int level) {
		FormalParameter[] fps=query.getFormalParameters();
		int paramCount=fps.length;
		Object[] parameters = new Object[paramCount];
		ArrayList<Integer> productFeatureIndices = new ArrayList<Integer>();
		ProductType pt = null;
		GregorianCalendar randomDate = null;
		Integer index = null, index1 = null;
		Integer horoscope = null;
		
		for (int i=0; i<paramCount; i++) {
			SIBFormalParameter fp=(SIBFormalParameter) fps[i];
			byte parameterType = fp.parameterType;
			switch (parameterType) {
            case PERSON_NAME: {
				parameters[i] = getRandomPersonName();
				break;            	
            }
            case PERSON_URI: {
            	parameters[i] = getRandomPersonURI();
            	break;
            }
            case CREATION_POST_DATE: {
                CreationPostDateFP cpdfp=(CreationPostDateFP) fp;
                parameters[i] = cpdfp.getRandomCreationPostDate();
                break;
            }
            case TOTAL_DAYS_OF_CREATION_POST: {
                TotalDaysOfCreationPostFP tdocpfp=(TotalDaysOfCreationPostFP) fp;
                parameters[i] = tdocpfp.getPercenOfTotalDaysCreationPost();
                break;
            }
            case COUNTRY_PAIR: {
            	if (index == null)
            		index = valueGen.randomInt(0, countryPairs.length / 2 - 1);
                CountryPairFP cpfp=(CountryPairFP) fp;
                parameters[i] = cpfp.getCountry(index);
                break;
            }
            case TAG_URI: {
            	parameters[i] = getRandomTagURI();
            	break;
            }
            case COUNTRY_URI: {
            	parameters[i] = getRandomCountryURI();
            	break;
            }
            case HOROSCOPE_SIGN: {
            	if (horoscope == null)
            		horoscope = valueGen.randomInt(1, 12);
                HoroscopeFP hfp=(HoroscopeFP) fp;
                parameters[i] = hfp.getMonth(horoscope);
            	break;
            }
            case WORK_FROM_DATE: {
                WorkFromDateFP cpdfp=(WorkFromDateFP) fp;
                parameters[i] = cpdfp.getRandomWorkFromDate();
                break;
            }
            case TAG_TYPE_URI: {
				parameters[i] = getRandomTagType();
				break;            	
            }
            case BIG_COUNTRY_URI: {
            	parameters[i] = getRandomBigCountryURI();
            	break;
            }
            case TAG_AND_NAME: {
            	if (index1 == null)
            		index1 = valueGen.randomInt(0, tagAndNameList.length / 2 - 1);
                TagAndNameFP tanfp=(TagAndNameFP) fp;
                parameters[i] = tanfp.getTagOrName(index1);
                break;
            }
            case FAMOUS_PERSON: {
				parameters[i] = getRandomFamousPerson();
				break;            	
            }
            case CITY_NAME: {
            	parameters[i] = getRandomCityName();
				break; 
            }
			default:
				parameters[i] = null;
			}
		}
		
		if(productFeatureIndices.size()>0 && pt == null) {
			throw new BadSetupException("Error in parameter generation: Asked for product features without product type.");
		}
		
		String[] productFeatures = getRandomProductFeatures(pt, productFeatureIndices.size());
		for(int i=0;i<productFeatureIndices.size();i++) {
			parameters[productFeatureIndices.get(i)] = productFeatures[i];
		}
		
		return parameters;
	}
	
    /*
     * Get date string for ConsecutiveMonth
     */
    private String getConsecutiveMonth(GregorianCalendar date, int monthNr) {
        GregorianCalendar gClone = (GregorianCalendar)date.clone();
        gClone.add(GregorianCalendar.DAY_OF_MONTH, 28*monthNr);
        return DateGenerator.formatDate(gClone);
    }
    
    /*
     * Get number distinct random Product Feature URIs of a certain Product Type
     */
    private String[] getRandomProductFeatures(ProductType pt, Integer number) {
        ArrayList<Integer> pfs = new ArrayList<Integer>();
        String[] productFeatures = new String[number];
        
        ProductType temp = pt;
        while(temp!=null) {
            List<Integer> tempList = temp.getFeatures();
            if(tempList!=null)
                pfs.addAll(temp.getFeatures());
            temp = temp.getParent();
        }
        
        if(pfs.size() < number) {
            DoubleLogger.getErr().println(pt.toString(), " doesn't contain ", number , " different Product Features!");
            System.exit(-1);
        }
        
        for(int i=0;i<number;i++) {
            Integer index = valueGen.randomInt(0, pfs.size()-1);
            productFeatures[i] = ProductFeature.getURIref(pfs.get(index));
            pfs.remove(index);
        }
        
        return productFeatures;
    }
    
    /*
     * Get a random date from (today-365) to (today-56)
     */
    private GregorianCalendar getRandomDate() {
        Integer dayOffset = valueGen.randomInt(0, 309);
        GregorianCalendar gClone = (GregorianCalendar)publishDateMin.clone();
        gClone.add(GregorianCalendar.DAY_OF_MONTH, dayOffset);
        return gClone;
    }
	
	/*
	 * Get a random person name
	 */
	private String getRandomPersonName() {
		Integer i = valueGen.randomInt(0, nameList.length-1);
		
		return nameList[i];
	}

	/*
	 * Get a random person URI
	 */
	private String getRandomPersonURI() {
		Integer i = valueGen.randomInt(0, maxNumberOfPerson);
		
		return "sn:pers" + i;
	}	

	/*
	 * Get a random tag URI
	 */
	private String getRandomTagURI() {
		Integer i = valueGen.randomInt(0, tagList.length - 1);
		
		return "dbpedia:" + tagList[i];
	}	
	
	/*
	 * Get a random country URI
	 */
	private String getRandomCountryURI() {
		Integer i = valueGen.randomInt(0, countryList.length - 1);
		
		return "dbpedia:" + countryList[i];
	}	

	/*
	 * Get a random tag type
	 */
	private String getRandomTagType() {
		Integer i = valueGen.randomInt(0, tagTypeList.length-1);
		
		return tagTypeList[i];
	}
	
	/*
	 * Get a random big country URI
	 */
	private String getRandomBigCountryURI() {
		Integer i = valueGen.randomInt(0, bigCountryList.length - 1);
		
		return "dbpedia:" + bigCountryList[i];
	}
	
	/*
	 * Get a random famous person
	 */
	private String getRandomFamousPerson() {
		Integer i = valueGen.randomInt(0, famousPersonList.length-1);
		
		return "dbpedia:" + famousPersonList[i];
	}

	/*
	 * Get a random city name
	 */
	private String getRandomCityName() {
		Integer i = valueGen.randomInt(0, cityNameList.length-1);
		
		return "dbpedia:" + cityNameList[i];
	}

	
	/*
	 * Return the triples to inserted into the store
	 */
	private String getUpdateTransactionData() {
		StringBuilder s = new StringBuilder();
		String line = null;
		if(updateFileReader==null) {
			throw new BadSetupException("Error: No update dataset file specified! Use -udataset option of the test driver with a generated update dataset file as argument.");
		}
		
		try {
			while((line=updateFileReader.readLine()) != null) {
				if(line.equals("#__SEP__"))
					break;
				s.append(line);
				s.append("\n");
			}
		} catch (IOException e) {
			throw new ExceptionException("Error reading update data from file: " + updateDatasetFile.getAbsolutePath(), e);
		}
		return s.toString();
	}

	@Override
	protected String formatDateString(GregorianCalendar date) {
		return "\"" + DateGenerator.formatDateTime(date) + "\"^^<" + XSD.DateTime + ">";
	}
}

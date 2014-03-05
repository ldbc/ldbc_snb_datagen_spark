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
package com.openlinksw.bibm.sib;

import java.io.File;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;

import com.openlinksw.bibm.FormalParameter;
import com.openlinksw.bibm.Query;
import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.util.DoubleLogger;

import benchmark.generator.DateGenerator;
import benchmark.model.ProductType;

public class SQLParameterPool extends SIBParameterPool {
	
	public SQLParameterPool(File resourceDirectory, Long seed) {
		parameterChar='@';
		init(resourceDirectory, seed);
	}

	/*
	 * (non-Javadoc)
	 * @see benchmark.testdriver.AbstractParameterPool#getParametersForQuery(benchmark.testdriver.Query)
	 */
	@Override
    public Object[] getParametersForQuery(Query query, int level) {
		FormalParameter[] fps=query.getFormalParameters();
		int paramCount=fps.length;
		Object[] parameters = new Object[paramCount];

		if (query.getName().equals("profile")) {
			StringBuilder tmp = new StringBuilder(listOfPeople.pop());
			tmp.setCharAt(0, ' ');
			tmp.setCharAt(tmp.length() - 1, ' ');
			parameters[0] = tmp.toString();
			return parameters;
		}

		ArrayList<Integer> productFeatureIndices = new ArrayList<Integer>();
		ProductType pt = null;
		Integer index = null, index1 = null;
		Integer horoscope = null;
		
		for(int i=0;i<paramCount;i++) {
			SIBFormalParameter fp=(SIBFormalParameter) fps[i];
			byte parameterType = fp.parameterType;
			switch (parameterType) {
			case PERSON_NAME: {
			    parameters[i] = getRandomPersonName();
			    break;            	
			}
			case PERSON_ID: {
			    parameters[i] = getRandomPersonID();
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
			case COUNTRY_PAIR_SQL: {
			    if (index == null)
				index = valueGen.randomInt(0, countryPairs.length / 2 - 1);
			    CountryPairSQLFP cpfp=(CountryPairSQLFP) fp;
			    parameters[i] = cpfp.getCountry(index);
			    break;
			}
			case TAG_SQL_URI: {
			    parameters[i] = getRandomTagSQLURI();
			    break;
			}
			case COUNTRY_SQL_URI: {
			    parameters[i] = getRandomCountrySQLURI();
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
			case TAG_TYPE_SQL_URI: {
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
			case PROFILE_VIEW_QUERY: {
			    ProfileViewQueryFP pvqfp=(ProfileViewQueryFP) fp;
			    parameters[i] = pvqfp.getProfileViewQuery(this.resourceDir);
			    break;
			}
			default:
			    parameters[i] = null;
			}
		}
		
		if(productFeatureIndices.size()>0 && pt == null) {
			throw new BadSetupException("Error in parameter generation: Asked for product features without product type.");
		}
		
		Integer[] productFeatures = getRandomProductFeatures(pt, productFeatureIndices.size());
		for(int i=0;i<productFeatureIndices.size();i++) {
			parameters[productFeatureIndices.get(i)] = productFeatures[i];
		}
		
		return parameters;
	}
	
    /*
     * Get number distinct random Product Feature URIs of a certain Product Type
     */
    private Integer[] getRandomProductFeatures(ProductType pt, Integer number) {
        ArrayList<Integer> pfs = new ArrayList<Integer>();
        Integer[] productFeatures = new Integer[number];
        
        ProductType temp = pt;
        while(temp!=null) {
            List<Integer> tempList = temp.getFeatures();
            if(tempList!=null)
                pfs.addAll(tempList);
            temp = temp.getParent();
        }
        
        if(pfs.size() < number) {
            DoubleLogger.getErr().println(pt.toString(), " doesn't contain ", number ," different Product Features!");
            System.exit(-1);
        }
        
        for(int i=0;i<number;i++) {
            Integer index = valueGen.randomInt(0, pfs.size()-1);
            productFeatures[i] = pfs.get(index);
            pfs.remove(index);
        }
        
        return productFeatures;
    }
		
	@Override
	protected String formatDateString(GregorianCalendar date) {
		return DateGenerator.formatDate(currentDate);
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
	private String getRandomPersonID() {
		Integer i = valueGen.randomInt(0, maxNumberOfPerson);
		
		return i.toString();
	}	

	/*
	 * Get a random tag URI
	 */
	private String getRandomTagSQLURI() {
		Integer i = valueGen.randomInt(0, tagList.length - 1);
		
		//return tagList[i].substring(0, tagList[i].length());
		return tagList[i];
	}	
	
	/*
	 * Get a random country URI
	 */
	private String getRandomCountrySQLURI() {
		Integer i = valueGen.randomInt(0, countryList.length - 1);
		
		return countryList[i];
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
		
		return bigCountryList[i];
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

}

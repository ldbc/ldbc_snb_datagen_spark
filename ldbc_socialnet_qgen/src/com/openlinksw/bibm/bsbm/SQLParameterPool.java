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
package com.openlinksw.bibm.bsbm;

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

public class SQLParameterPool extends BSBMParameterPool {
	
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
		ArrayList<Integer> productFeatureIndices = new ArrayList<Integer>();
		ProductType pt = null;
		
		for(int i=0;i<paramCount;i++) {
			BSBMFormalParameter fp=(BSBMFormalParameter) fps[i];
			byte parameterType = fp.parameterType;
			if(parameterType==PRODUCT_TYPE_URI) {
				pt = getRandomProductType();
				parameters[i] = pt.getNr();
			}
			else if(parameterType==PRODUCT_FEATURE_URI)
				productFeatureIndices.add(i);
			else if(parameterType==PRODUCT_PROPERTY_NUMERIC)
				parameters[i] = getProductPropertyNumeric();
			else if(parameterType==PRODUCT_URI)
				parameters[i] = getRandomProductURI();
			else if(parameterType==CURRENT_DATE)
				parameters[i] = currentDateString;
			else if(parameterType==COUNTRY_URI)
				parameters[i] = countryGen.getRandom();
			else if(parameterType==REVIEW_URI)
				parameters[i] = getRandomReviewURI();
			else if(parameterType==WORD_FROM_DICTIONARY1)
				parameters[i] = getRandomWord();
			else if(parameterType==OFFER_URI)
				parameters[i] = getRandomOfferURI();
			else
				parameters[i] = null;
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
	
	/*
	 * Get a random Product Type URI
	 */
	private ProductType getRandomProductType() {
		Integer index = valueGen.randomInt(0, productTypeLeaves.length-1);
		
		return productTypeLeaves[index];
	}
	
	/*
	 * Get a random Product URI
	 */
	private Integer getRandomProductURI() {
		return valueGen.randomInt(1, productCount);
	}
	
	/*
	 * Get a random Offer URI
	 */
	private Integer getRandomOfferURI() {
		return valueGen.randomInt(1, offerCount);
	}
	
	/*
	 * Get a random Review URI
	 */
	private Integer getRandomReviewURI() {
		return valueGen.randomInt(1, reviewCount);
	}
	
	
	@Override
	protected String formatDateString(GregorianCalendar date) {
		return DateGenerator.formatDate(currentDate);
	}

}

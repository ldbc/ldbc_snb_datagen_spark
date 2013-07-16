package com.openlinksw.bibm.bsbm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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

public class LocalSPARQLParameterPool extends BSBMParameterPool {
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
		
		for (int i=0; i<paramCount; i++) {
			BSBMFormalParameter fp=(BSBMFormalParameter) fps[i];
			byte parameterType = fp.parameterType;
			switch (parameterType) {
			case PRODUCT_TYPE_URI: 
				pt = getRandomProductType();
				parameters[i] = pt.toString();
				break;
			case PRODUCT_FEATURE_URI:
				productFeatureIndices.add(i);
				break;
			case PRODUCT_PROPERTY_NUMERIC:
				parameters[i] = getProductPropertyNumeric();
				break;
			case PRODUCT_URI:
				parameters[i] = getRandomProductURI();
				break;
			case CURRENT_DATE:
				parameters[i] = currentDateString;
				break;
			case COUNTRY_URI:
				parameters[i] = "<" + ISO3166.find((String)countryGen.getRandom()) + ">";
				break;
			case REVIEW_URI:
				parameters[i] = getRandomReviewURI();
				break;
			case WORD_FROM_DICTIONARY1:
				parameters[i] = getRandomWord();
				break;
			case OFFER_URI:
				parameters[i] = getRandomOfferURI();
				break;
			case UPDATE_TRANSACTION_DATA:
				parameters[i] = getUpdateTransactionData();
				break;
			case CONSECUTIVE_MONTH: {
				if(randomDate==null)
					randomDate = getRandomDate();
				ConsecMonth cm=(ConsecMonth)fp;
				parameters[i] = getConsecutiveMonth(randomDate, cm.monthNr);
				break;
			}
			case PRODUCER_URI:
				parameters[i] = getRandomProducerURI();
				break;
			case PRODUCT_TYPE_RANGE: {
				ProdTypeLevelRange range =  (ProdTypeLevelRange)fp;
				int ptnr;
				if (level>-1 && fp==query.getProdTypeLevelRange()) {
					ptnr = range.getRandomProductTypeNrFromLevel(valueGen, level, level);
				} else {
					ptnr = range.getRandomProductTypeNrFromRange(valueGen, valueGen2);
				}
				parameters[i] = ProductType.getURIRef(ptnr);
				break;
			}
            case LITERAL: 
            case LITERAL_V: {
                LiteralObject lo=(LiteralObject)fp;
                parameters[i] = lo.getRandomSelectivitySentence();
                break;
              } 
            case RESTRICTED_PROPERTY: {
                RestrictedProperty rp=(RestrictedProperty)fp;
                Object[] restrictions = rp.getRestrictions();
                
                if (!restrictionsBucket.containsKey(i)) {
                    restrictionsBucket.put(i, new RandomBucket(restrictions.length >> 1, seedGen.nextLong()));
                    for (int j = 0; j < restrictions.length; j += 2) {
                        restrictionsBucket.get(i).add((Integer) restrictions[j + 1], restrictions[j]);
                    }
                }
                parameters[i] = restrictionsBucket.get(i).getRandom();
                break;
             }
            case SELECTIVITY: {
                SelectivityFP sfp=(SelectivityFP)fp;
                parameters[i] = sfp.getRandomSelectivityWord();
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
     * Get a random Product Type URI
     */
    private ProductType getRandomProductType() {
        Integer index = valueGen.randomInt(0, productTypeLeaves.length-1);
        
        return productTypeLeaves[index];
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
	 * Get a random Product URI
	 */
	private String getRandomProductURI() {
		Integer productNr = getRandomProductNr();
		Integer producerNr = getProducerOfProduct(productNr);
		
		return Product.getURIref(productNr, producerNr);
	}
	
	/*
	 * Get a random Offer URI
	 */
	private String getRandomOfferURI() {
		Integer offerNr = valueGen.randomInt(1, offerCount);
		Integer vendorNr = getVendorOfOffer(offerNr);
		
		return Offer.getURIref(offerNr, vendorNr);
	}
	
	/*
	 * Get a random Review URI
	 */
	private String getRandomReviewURI() {
		Integer reviewNr = valueGen.randomInt(1, reviewCount);
		Integer ratingSiteNr = getRatingsiteOfReviewer(reviewNr);
		
		return Review.getURIref(reviewNr, ratingSiteNr);
	}
	
	/*
	 * Get a random producer URI
	 */
	private String getRandomProducerURI() {
		Integer producerNr = valueGen.randomInt(1, producerOfProduct.length-1);
		
		return Producer.getURIref(producerNr);
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

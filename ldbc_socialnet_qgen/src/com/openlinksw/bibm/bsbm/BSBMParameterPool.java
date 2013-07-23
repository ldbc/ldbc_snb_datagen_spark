package com.openlinksw.bibm.bsbm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
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

public abstract class BSBMParameterPool extends AbstractParameterPool {
    static final int baseScaleFactor = 284826; // ~100 000 triples is the
                                               // standard database size

    // Parameter constants
    protected static final byte PRODUCT_PROPERTY_NUMERIC = 1;
    protected static final byte PRODUCT_FEATURE_URI = 2;
    protected static final byte PRODUCT_TYPE_URI = 3;
    protected static final byte CURRENT_DATE = 4;
    protected static final byte WORD_FROM_DICTIONARY1 = 5;
    protected static final byte PRODUCT_URI = 6;
    protected static final byte REVIEW_URI = 7;
    protected static final byte COUNTRY_URI = 8;
    protected static final byte OFFER_URI = 9;
    protected static final byte CONSECUTIVE_MONTH = 10;
    protected static final byte UPDATE_TRANSACTION_DATA = 11;
    protected static final byte PRODUCER_URI = 12;
    protected static final byte PRODUCT_TYPE_RANGE = 13;
    protected static final byte LITERAL = 14;
    protected static final byte RESTRICTED_PROPERTY = 15;
    protected static final byte LITERAL_V = 16;
    protected static final byte SELECTIVITY = 17;

    // Initialize Parameter mappings
    private static Map<String, Byte> parameterMapping;
    static {
        parameterMapping = new HashMap<String, Byte>();
        parameterMapping.put("ProductPropertyNumericValue", PRODUCT_PROPERTY_NUMERIC);
        parameterMapping.put("ProductFeatureURI", PRODUCT_FEATURE_URI);
        parameterMapping.put("ProductTypeURI", PRODUCT_TYPE_URI);
        parameterMapping.put("CurrentDate", CURRENT_DATE);
        parameterMapping.put("Dictionary1", WORD_FROM_DICTIONARY1);
        parameterMapping.put("ProductURI", PRODUCT_URI);
        parameterMapping.put("ReviewURI", REVIEW_URI);
        parameterMapping.put("CountryURI", COUNTRY_URI);
        parameterMapping.put("OfferURI", OFFER_URI);
        parameterMapping.put("ConsecutiveMonth", CONSECUTIVE_MONTH);
        parameterMapping.put("UpdateTransactionData", UPDATE_TRANSACTION_DATA);
        parameterMapping.put("ProducerURI", PRODUCER_URI);
        parameterMapping.put("ProductTypeRangeURI", PRODUCT_TYPE_RANGE);
        parameterMapping.put("Literal", LITERAL);
        parameterMapping.put("RestrictedProperty", RESTRICTED_PROPERTY);
        parameterMapping.put("LiteralV", LITERAL_V);
        parameterMapping.put("Selectivity", SELECTIVITY);
    }

    protected ValueGenerator valueGen;
    protected ValueGenerator valueGen2;
    protected RandomBucket countryGen;
    protected GregorianCalendar currentDate;
    protected String currentDateString;
    protected ProductType[] productTypeLeaves;
    protected HashMap<String, Integer> wordHash;
    protected String[] wordList;
    protected Integer[] producerOfProduct;
    protected Integer[] vendorOfOffer;
    protected Integer[] ratingsiteOfReview;
    protected Integer productCount;
    protected Integer reviewCount;
    protected Integer offerCount;
    protected int productTypeCount;
    protected List<Integer> maxProductTypePerLevel;
    private Map<Selectivity, String[]> anyProperty1gram;
    private Map<Selectivity, String[]> anyProperty2gram;
    protected HashMap<Integer, RandomBucket> restrictionsBucket = new HashMap<Integer, RandomBucket>();
    final StringBuilder sb = new StringBuilder();
    protected Random seedGen;

    protected double scalefactor;

    public double getScalefactor() {
        return scalefactor / baseScaleFactor;
    }

    protected void init(File resourceDir, long seed) {
        seedGen = new Random(seed);
        valueGen = new ValueGenerator(seedGen.nextLong());

        countryGen = Generator.createCountryGenerator(seedGen.nextLong());

        valueGen2 = new ValueGenerator(seedGen.nextLong());

        // Read in the Product Type hierarchy from resourceDir/pth.dat
        readProductTypeHierarchy(resourceDir);

        // Product-Producer Relationships from resourceDir/pp.dat
        File pp = readProductProducerData(resourceDir);

        // Offer-Vendor Relationships from resourceDir/vo.dat
        readOfferAndVendorData(resourceDir, pp);

        // Review-Rating Site Relationships from resourceDir/rr.dat
        readReviewSiteData(resourceDir);

        // Current date and words of Product labels from resourceDir/cdlw.dat
        readDateAndLabelWords(resourceDir);

        // anyProperty selectivity ranges
        readAnyPropertySelectivityRanges(resourceDir);
    }

    private void readSelectivityRanges(Map<Selectivity, String[]> sel, ObjectInputStream in) throws IOException, ClassNotFoundException {
        sel.put(Selectivity.HIGH, (String[]) in.readObject());
        sel.put(Selectivity.MEDIUM, (String[]) in.readObject());
        sel.put(Selectivity.LOW, (String[]) in.readObject());
    }

    private void readAnyPropertySelectivityRanges(File resourceDir) {
        File anylw = new File(resourceDir, "anylw.dat");
        if (!anylw.exists()) {
            return;
        }
        anyProperty1gram = new HashMap<Selectivity, String[]>();
        anyProperty2gram = new HashMap<Selectivity, String[]>();
        ObjectInputStream anyPropertyWordsInput;
        try {
            anyPropertyWordsInput = new ObjectInputStream(new FileInputStream(anylw));
            readSelectivityRanges(anyProperty1gram, anyPropertyWordsInput);
            readSelectivityRanges(anyProperty2gram, anyPropertyWordsInput);
            anyPropertyWordsInput.close();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + anylw.getAbsolutePath(), e);
        } catch (ClassNotFoundException e) {
            // Cannot happen, in Generator.java, String[] have been serialized
        }
    }

    private void readDateAndLabelWords(File resourceDir) {
        File cdlw = new File(resourceDir, "cdlw.dat");
        ObjectInputStream currentDateAndLabelWordsInput;
        try {
            currentDateAndLabelWordsInput = new ObjectInputStream(new FileInputStream(cdlw));
            productCount = currentDateAndLabelWordsInput.readInt();
            reviewCount = currentDateAndLabelWordsInput.readInt();
            offerCount = currentDateAndLabelWordsInput.readInt();
            currentDate = (GregorianCalendar) currentDateAndLabelWordsInput.readObject();
            currentDateString = formatDateString(currentDate);

            @SuppressWarnings("unchecked")
            HashMap<String, Integer> x = (HashMap<String, Integer>) currentDateAndLabelWordsInput.readObject();
            wordHash = x;
            wordList = wordHash.keySet().toArray(new String[0]);
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + cdlw.getAbsolutePath(), e);
        } catch (ClassNotFoundException e) {
            DoubleLogger.getErr().println(e);
        }
    }

    private void readReviewSiteData(File resourceDir) {
        File rr = new File(resourceDir, "rr.dat");
        ObjectInputStream reviewRatingsiteInput;
        try {
            reviewRatingsiteInput = new ObjectInputStream(new FileInputStream(rr));
            ratingsiteOfReview = (Integer[]) reviewRatingsiteInput.readObject();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + rr.getAbsolutePath(), e);
        } catch (ClassNotFoundException e) {
            DoubleLogger.getErr().println(e);
        }
    }

    private void readOfferAndVendorData(File resourceDir, File pp) {
        File vo = new File(resourceDir, "vo.dat");
        ObjectInputStream offerVendorInput;
        try {
            offerVendorInput = new ObjectInputStream(new FileInputStream(vo));
            vendorOfOffer = (Integer[]) offerVendorInput.readObject();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + pp.getAbsolutePath(), e);
        } catch (ClassNotFoundException e) {
            DoubleLogger.getErr().println(e);
        }
    }

    private File readProductProducerData(File resourceDir) {
        File pp = new File(resourceDir, "pp.dat");
        ObjectInputStream productProducerInput;
        try {
            productProducerInput = new ObjectInputStream(new FileInputStream(pp));
            producerOfProduct = (Integer[]) productProducerInput.readObject();
            scalefactor = producerOfProduct[producerOfProduct.length - 1];
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + pp.getAbsolutePath(), e);
        } catch (ClassNotFoundException e) {
            DoubleLogger.getErr().println(e);
        }
        return pp;
    }

    @SuppressWarnings("unchecked")
    private void readProductTypeHierarchy(File resourceDir) {
        ObjectInputStream productTypeInput;
        File pth = new File(resourceDir, "pth.dat");
        try {
            productTypeInput = new ObjectInputStream(new FileInputStream(pth));
            productTypeLeaves = (ProductType[]) productTypeInput.readObject();
            productTypeCount = productTypeInput.readInt();
            maxProductTypePerLevel = (List<Integer>) productTypeInput.readObject();
        } catch (IOException e) {
            throw new ExceptionException("Could not open or process file " + pth.getAbsolutePath(), e);
        } catch (ClassNotFoundException e) {
            DoubleLogger.getErr().println(e);
        }
    }

    public BSBMFormalParameter createFormalParameter(String paramClass, String[] addPI, String defaultValue) {
        Byte byteType = parameterMapping.get(paramClass);
        if (byteType == null) {
            throw new BadSetupException("Unknown parameter class: " + paramClass);
        }
        switch (byteType) {
        case CONSECUTIVE_MONTH:
            return new ConsecMonth(addPI);
        case PRODUCT_TYPE_RANGE:
            return new ProdTypeLevelRange(this, addPI);
        case LITERAL:
        case LITERAL_V:
            if (anyProperty1gram==null) {
                throw new BadSetupException("Setup error: a query has Literal parameter but anylw.dat file is absent. Run data generator with -pareto option.");
            }
            return byteType==LITERAL?  new LiteralObject(this, addPI): new LiteralObjectVirtuoso(this, addPI);
        case RESTRICTED_PROPERTY:
            return new RestrictedProperty(this, addPI);
        case SELECTIVITY:
            return new SelectivityFP(this, addPI);
        default:
            return new BSBMFormalParameter(byteType);
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

    String getRandomSelectivityWord(NGram gram, Selectivity sel) {
        final String[] terms;

        switch (gram) {
        case GRAM1:
            terms = anyProperty1gram.get(sel);
            return terms[valueGen.randomInt(0, terms.length - 1)];
        case GRAM2:
            terms = anyProperty2gram.get(sel);
            return terms[valueGen.randomInt(0, terms.length - 1)];
        default:
            DoubleLogger.getErr().println("No WordList for the ngram: ", gram);
            System.exit(-1);
            return null;
        }
    }

    /*
     * Get a random Product URI
     */
    protected Integer getRandomProductNr() {
        Integer productNr = valueGen.randomInt(1, productCount);

        return productNr;
    }

    /*
     * Returns the ProducerNr of given Product Nr.
     */
    protected Integer getProducerOfProduct(Integer productNr) {
        Integer producerNr = Arrays.binarySearch(producerOfProduct, productNr);
        if (producerNr < 0)
            producerNr = -producerNr - 1;

        return producerNr;
    }

    /*
     * Returns the ProducerNr of given Product Nr.
     */
    protected Integer getVendorOfOffer(Integer offerNr) {
        Integer vendorNr = Arrays.binarySearch(vendorOfOffer, offerNr);
        if (vendorNr < 0)
            vendorNr = -vendorNr - 1;

        return vendorNr;
    }

    /*
     * Returns the Rating Site Nr of given Review Nr
     */
    protected Integer getRatingsiteOfReviewer(Integer reviewNr) {
        Integer ratingSiteNr = Arrays.binarySearch(ratingsiteOfReview, reviewNr);
        if (ratingSiteNr < 0)
            ratingSiteNr = -ratingSiteNr - 1;

        return ratingSiteNr;
    }

    /*
     * Returns a random number between 1-500
     */
    protected Integer getProductPropertyNumeric() {
        return valueGen.randomInt(1, 500);
    }

    /*
     * Get random word from word list
     */
    protected String getRandomWord() {
        Integer index = valueGen.randomInt(0, wordList.length - 1);

        return wordList[index];
    }

    static class ConsecMonth extends BSBMFormalParameter {
        int monthNr;

        public ConsecMonth(String[] addPI) {
            super(CONSECUTIVE_MONTH);
            try {
                this.monthNr = Integer.parseInt(addPI[0]);
            } catch (NumberFormatException e) {
                throw new BadSetupException("Illegal parameter for ConsecutiveMonth: " + addPI[0]);
            }
        }

    }

    /**
     * variable=the one that this Literal boolean expression describe
     * type=[gram1|gram2|...] the dictionary to use op=[AND|OR] the operator
     * n=<INTEGER> the number of terms to generate S=[LOW|MEDIUM|HIGH]{n} the
     * selectivity of each term
     */
    static class LiteralObject extends BSBMFormalParameter {
        BSBMParameterPool parameterPool;
        String variable;
        NGram ngram;
        Operator op;
        Selectivity[] selectivities;

        public LiteralObject(BSBMParameterPool parameterPool, String[] addPI) {
            super(BSBMParameterPool.LITERAL);
            this.parameterPool = parameterPool;
            Object[] params = new Object[addPI.length];
            try {
                variable = addPI[0];
                ngram = NGram.valueOf(addPI[1]);
                op = Operator.valueOf(addPI[2]);
                int length = Integer.parseInt(addPI[3]);
                int nSel = addPI.length - 4;
                if (length != nSel)
                    throw new IllegalArgumentException("You have to specify as much Selectivity as there are terms: " + params[3] + " terms for " + nSel
                            + " Selectivities.");
                selectivities = new Selectivity[nSel];
                for (int i =0; i < selectivities.length; i++)
                    selectivities[i] = Selectivity.valueOf(addPI[i+4]);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for Literal: " + addPI);
            }
        }

        /**
         * Creates a random sentence of Terms with the given Selectivities. The
         * terms will be taken from a specific ngram dictionary, and the terms
         * will be associated with the operator OP.
         * 
         * @return
         */
        public Object getRandomSelectivitySentence() {
            StringBuffer sb = new StringBuffer();
            sb.setLength(0);
            String operator = getOperator(op);
            for (int i = 0; i < selectivities.length; i++) {
                if (i>0) {
                    sb.append(operator);
                }
                String randomSelectivityWord = parameterPool.getRandomSelectivityWord(ngram, selectivities[i]);
                sb.append("regex(?").append(variable).append(", \"").append(randomSelectivityWord).append("\")");
            }
            return sb.toString();
        }

        protected String getOperator(Operator op) {
            switch (op) {
            case AND:
                return " && ";
            case OR:
                return " || ";
            default:
                throw new IllegalArgumentException("The Operator " + op + " does not exist");
            }
        }

    }

    static class LiteralObjectVirtuoso extends LiteralObject {
        public LiteralObjectVirtuoso(BSBMParameterPool parameterPool, String[] addPI) {
            super(parameterPool, addPI);
        }

        /**
         * Creates a random sentence of Terms with the given Selectivities. The
         * terms will be taken from a specific ngram dictionary, and the terms
         * will be associated with the operator OP.
         * 
         * @return
         */
        public Object getRandomSelectivitySentence() {
            StringBuffer sb = new StringBuffer();
            sb.setLength(0);
            String operator = getOperator(op);
            sb.append("?").append(variable).append(" bif:contains \"");
            for (int i = 0; i < selectivities.length; i++) {
                if (i>0) {
                    sb.append(operator);
                }
                String randomSelectivityWord = parameterPool.getRandomSelectivityWord(ngram, selectivities[i]);
                sb.append("'").append(randomSelectivityWord).append("'");
            }
            sb.append("\"");
            return sb.toString();
        }

        protected String getOperator(Operator op) {
            switch (op) {
            case AND:
                return " and ";
            case OR:
                return " or ";
            default:
                throw new IllegalArgumentException("The Operator " + op + " does not exist");
            }
        }

    }

    static class RestrictedProperty extends BSBMFormalParameter {
        BSBMParameterPool parameterPool;
        Object[] restrictions;

        public RestrictedProperty(BSBMParameterPool parameterPool, String[] addPI) {
            super(BSBMParameterPool.RESTRICTED_PROPERTY);
            this.parameterPool = parameterPool;
            restrictions = new Object[addPI.length];
            try {
                int percentage = 0;
                for (int i = 0; i < restrictions.length; i += 2) {
                    restrictions[i] = addPI[i];
                    restrictions[i + 1] = Integer.parseInt(addPI[i + 1]);
                    percentage += (Integer) restrictions[i + 1];
                }
                if (percentage != 100)
                    throw new IllegalArgumentException("The percentage of the property to appear do not sum up to 100");
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for RestrictedProperty: " + addPI);
            }
        }

        public Object[] getRestrictions() {
            return restrictions;
        }
    }

    static class SelectivityFP extends BSBMFormalParameter {
        BSBMParameterPool parameterPool;
        NGram ngram;
        Selectivity selectivity;

        public SelectivityFP(BSBMParameterPool parameterPool, String[] addPI) {
            super(BSBMParameterPool.SELECTIVITY);
            this.parameterPool = parameterPool;
            try {
                if (addPI.length != 2)
                    throw new IllegalArgumentException("Illegal parameters numbers for Selectivity: "+addPI.length+" (expected:2)");
                ngram = NGram.valueOf(addPI[0]);
                selectivity = Selectivity.valueOf(addPI[1]);
            } catch (IllegalArgumentException e) {
                throw new BadSetupException("Illegal parameters for Selectivity: " + addPI);
            }
        }

        /**
         * Creates a random sentence of Terms with the given Selectivities. The
         * terms will be taken from a specific ngram dictionary, and the terms
         * will be associated with the operator OP.
         * 
         * @return
         */
        public Object getRandomSelectivityWord() {
            return parameterPool.getRandomSelectivityWord(ngram, selectivity);
        }
    }

}

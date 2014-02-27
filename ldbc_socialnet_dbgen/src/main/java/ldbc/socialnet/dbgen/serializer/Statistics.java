package ldbc.socialnet.dbgen.serializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeSet;

import ldbc.socialnet.dbgen.dictionary.LocationDictionary;
import ldbc.socialnet.dbgen.objects.FlashmobTag;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;

/**
 * Container class used to store all the generator statistics.
 *
 */
public class Statistics {
    
    /**
     * This was used in early states to exclude a field of a class in the gson parser library. 
     * It is keep for the sake of having a quick example/place to write future field exclusions.
     */
    public class StatisticsExclude implements ExclusionStrategy {

        public boolean shouldSkipClass(Class<?> arg0) {
            return false;
        }

        public boolean shouldSkipField(FieldAttributes f) {

            return (f.getDeclaringClass() == CountryPair.class && f.getName().equals("population"));
        }

    }
    
    /**
     * Container to keep paired countries and their population. It is used to sort
     * this pairs by population.
     */
    private class CountryPair implements Comparable<CountryPair> {
        public String[] countries = new String[2];
        public Long population;
        
        
        public int compareTo(CountryPair pair) {
            return pair.population.compareTo(population);
        }
    }
    
    public Integer minPersonId;
    public Integer maxPersonId;
    public String minWorkFrom;
    public String maxWorkFrom;
    public String minPostCreationDate;
    public String maxPostCreationDate;
    public TreeSet<String> firstNames;
    public TreeSet<String> tagNames;
    public TreeSet<String> countries;
    public TreeSet<String> tagClasses;
    public FlashmobTag[] flashmobTags;
    private ArrayList<String[]> countryPairs;
    
    public Statistics() {
        minPersonId = Integer.MAX_VALUE;
        maxPersonId = Integer.MIN_VALUE;
        firstNames = new TreeSet<String>();
        tagNames = new TreeSet<String>();
        tagClasses = new TreeSet<String>();
        countries = new TreeSet<String>();
        flashmobTags = null; 
        countryPairs = new ArrayList<String[]>();
    }
    
    /**
     * Makes all the country pairs of the countries found in the countries statistic field belonging
     * to the same continent. This pairs are sorted by population.
     * 
     * @param dicLocation The location dictionary.
     */
    public void makeCountryPairs(LocationDictionary dicLocation) {
        HashMap<Integer, ArrayList<Integer>> closeCountries = new HashMap<Integer, ArrayList<Integer>>();
        for (String s : countries) {
            Integer id = dicLocation.getCountryId(s);
            Integer continent = dicLocation.belongsTo(id);
            if (!closeCountries.containsKey(continent)) {
                closeCountries.put(continent, new ArrayList<Integer>());
            }
            closeCountries.get(continent).add(id);
        }
        
        ArrayList<CountryPair> toSort = new ArrayList<CountryPair>();
        for (ArrayList<Integer> relatedCountries : closeCountries.values()) {
            for (int i = 0; i < relatedCountries.size(); i++) {
                for (int j = i+1; j < relatedCountries.size(); j++) {
                    CountryPair pair = new CountryPair();
                    pair.countries[0] = dicLocation.getLocationName(relatedCountries.get(i));
                    pair.countries[1] = dicLocation.getLocationName(relatedCountries.get(j));
                    pair.population = dicLocation.getPopulation(relatedCountries.get(i)) 
                            + dicLocation.getPopulation(relatedCountries.get(j));
                    toSort.add(pair);
                }
            }
        }
        Collections.sort(toSort);
        for (CountryPair p : toSort) {
            countryPairs.add(p.countries);
        }
    }
    
    /**
     * Get gson exclusion class for the statistics.
     * 
     * @return The exclusion class.
     */
    public StatisticsExclude getExclusion() {
        return new StatisticsExclude();
    }
}

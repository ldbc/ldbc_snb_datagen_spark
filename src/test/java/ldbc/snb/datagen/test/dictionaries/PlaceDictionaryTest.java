package ldbc.snb.datagen.test.dictionaries;

import ldbc.snb.datagen.DatagenContext;
import ldbc.snb.datagen.dictionary.PlaceDictionary;
import ldbc.snb.datagen.hadoop.HadoopConfiguration;
import ldbc.snb.datagen.hadoop.LdbcDatagen;
import ldbc.snb.datagen.util.ConfigParser;
import ldbc.snb.datagen.util.LdbcConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertTrue;
public class PlaceDictionaryTest {

    @Test
    public void testPopulationDistribution() {

            PlaceDictionary placeDictionary = new PlaceDictionary();
        try {
            Map<String, String> confMap = ConfigParser.defaultConfiguration();
            confMap.putAll(ConfigParser.readConfig("./test_params.ini"));
            confMap.putAll(ConfigParser.readConfig(LdbcDatagen.class.getResourceAsStream("/params_default.ini")));
            LdbcConfiguration conf = new LdbcConfiguration(confMap);
            HadoopConfiguration.prepare(conf);
            DatagenContext.initialize(conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int numPersons = 10000000;
        int countryFreqs[] = new int[placeDictionary.getCountries().size()];
        Arrays.fill(countryFreqs, 0);
        Random random = new Random(123456789);
        for (int i = 0; i < numPersons; ++i) {
            int nextCountry = placeDictionary.getCountryForPerson(random);
            countryFreqs[nextCountry]++;
        }

        for( int i = 0; i < countryFreqs.length; ++i) {
            String countryName = placeDictionary.getPlaceName(i);
            int expectedPopulation = (int)(placeDictionary.getCumProbabilityCountry(i)*numPersons);
            int actualPopulation = countryFreqs[i];
            float error = Math.abs(expectedPopulation-actualPopulation)/(float)(expectedPopulation);
            assertTrue("Error in population of "+countryName+". Expected Population: "+expectedPopulation+", Actual " +
                               "Population: "+actualPopulation+". Error="+error,  error < 0.05) ;
        }
    }




}

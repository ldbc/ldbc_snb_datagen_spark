package ldbc.snb.datagen.generator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.objects.Person;

public abstract class TextGenerator {
	private static final String SEPARATOR = "  ";
    protected TagDictionary tagDic;
    protected Random random;
    /**
     * < @brief The tag dictionary. *
     */
    private HashMap<Integer, String> tagText;
    /**
     * < @brief The tag text. *
     */
    private double reducedTextRatio;
    
    private Person persona;

    /**
     * < @brief The probability to retrieve an small text.
     */

    public TextGenerator(Random random, TagDictionary tagDic) {
              this.tagDic = tagDic;
              this.random = random;
     }

    /**
     * @param fileName The tag text dictionary file name.
     * @brief Loads the dictionary.
     */
    protected abstract void load() ;
  
    /**
     * @param randomTextSize The random number generator to generate the amount of text devoted to each tag.
     * @param tags           The set of tags to generate the text from.
     * @param textSize       The final text size.
     * @return The final text.
     * @brief Generates a text given a set of tags.
     */
    //info extra
    public abstract String generateText(Person.PersonSummary person, TreeSet<Integer> tags, Properties prop);

}

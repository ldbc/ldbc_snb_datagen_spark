package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.objects.Person;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public abstract class TextGenerator {
	   protected TagDictionary tagDic;
    protected Random random;
    

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

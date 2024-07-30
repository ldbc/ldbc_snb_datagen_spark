package ldbc.snb.datagen.generator.vocabulary;

/**
 * RDF dbpedia resource namespace used in the serialization process.
 */
public class DBP {

    public static final String NAMESPACE = "http://dbpedia.org/resource/";
    public static final String PREFIX = "dbpedia:";

    /**
     * Gets the dbpedia resource URL version of the input.
     */
    public static String getUrl(String string) {
        return NAMESPACE + string;
    }

}

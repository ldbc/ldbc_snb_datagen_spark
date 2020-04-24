package ldbc.snb.datagen;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.util.LdbcConfiguration;
import ldbc.snb.datagen.vocabulary.SN;

public class DatagenContext {
    private static volatile transient boolean initialized = false;

    public static synchronized void initialize(LdbcConfiguration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries(conf);
            SN.initialize();
            Person.personSimilarity = DatagenParams.getPersonSimularity();
            initialized = true;
        }
    }
}

package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.generator.dictionary.Dictionaries;
import ldbc.snb.datagen.util.GeneratorConfiguration;
import ldbc.snb.datagen.generator.vocabulary.SN;

public class DatagenContext {
    private static volatile transient boolean initialized = false;

    public static synchronized void initialize(GeneratorConfiguration conf) {
        if (!initialized) {
            DatagenParams.readConf(conf);
            Dictionaries.loadDictionaries();
            SN.initialize();
            initialized = true;
        }
    }
}

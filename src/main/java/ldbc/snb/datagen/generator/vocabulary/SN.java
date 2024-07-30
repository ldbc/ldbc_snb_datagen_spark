package ldbc.snb.datagen.generator.vocabulary;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.dictionary.Dictionaries;

/**
 * LDBC social network data namespace used in the serialization process.
 */
public class SN {

    private static long numBits;
    private static long minDate;
    private static long maxDate;

    /**
     * Sets the machine id.
     * Used as a suffix in some SN entities' to create unique IDs in parallel generation.
     */
    public static void initialize() {
        minDate = Dictionaries.dates.getSimulationStart();
        maxDate = Dictionaries.dates.getSimulationEnd();
        numBits = (int) Math.ceil(Math.log10(Math.ceil(DatagenParams.numPersons / (double) DatagenParams.blockSize)) / Math.log10(2));
        if (numBits > 20) throw new IllegalStateException("Possible id overlap");
    }

    public static Long formId(long id, long blockId) {
        long lowMask = 0x0FFFFF;                                // This mask is used to get the lowest 20 bits.
        long lowerPart = (lowMask & id);
        long machinePart = blockId << 20;
        long upperPart = (id >> 20) << (20 + numBits);
        return upperPart | machinePart | lowerPart;
    }

    public static long composeId(long id, long creationDate) {
        long bucket = (long) (256 * (creationDate - minDate) / (double) maxDate);
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 36);
        return (bucket << 36) | (id & idMask);
    }

}

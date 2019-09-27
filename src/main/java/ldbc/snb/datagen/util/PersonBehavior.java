package ldbc.snb.datagen.util;

import ldbc.snb.datagen.generation.DateGenerator;

import java.util.Random;

import static ldbc.snb.datagen.generation.DatagenParams.probDiffIPinTravelSeason;
import static ldbc.snb.datagen.generation.DatagenParams.probDiffIPnotTravelSeason;

/**
 * Created by aprat on 27/07/17.
 */
public class PersonBehavior {

    public static boolean changeUsualCountry(Random random, long date) {
        double diffIpForTravelersProb = random.nextDouble();
        boolean isTravelSeason = DateGenerator.isTravelSeason(date);
        return (isTravelSeason && diffIpForTravelersProb < probDiffIPinTravelSeason) ||
                (!isTravelSeason && diffIpForTravelersProb < probDiffIPnotTravelSeason);
    }
}

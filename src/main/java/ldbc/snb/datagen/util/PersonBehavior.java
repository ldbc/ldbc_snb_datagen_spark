package ldbc.snb.datagen.util;

import java.util.Random;

import static ldbc.snb.datagen.DatagenParams.probDiffIPinTravelSeason;
import static ldbc.snb.datagen.DatagenParams.probDiffIPnotTravelSeason;

/**
 * Created by aprat on 27/07/17.
 */
public class PersonBehavior {

    public static boolean changeUsualCountry(Random random, long date) {
        double diffIpForTravelersProb = random.nextDouble();
        boolean isTravelSeason = DateUtils.isTravelSeason(date);
        return (isTravelSeason && diffIpForTravelersProb < probDiffIPinTravelSeason) ||
                (!isTravelSeason && diffIpForTravelersProb < probDiffIPnotTravelSeason);
    }
}

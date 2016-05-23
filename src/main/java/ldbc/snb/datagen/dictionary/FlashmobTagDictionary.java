/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.generator.DateGenerator;
import ldbc.snb.datagen.generator.PowerDistGenerator;
import ldbc.snb.datagen.objects.FlashmobTag;

import java.util.*;

public class FlashmobTagDictionary {

    private DateGenerator dateGen;
    /**
     * < @brief The date generator used to generate dates.
     */
    private PowerDistGenerator levelGenerator;
    /**
     * < @brief The powerlaw distribution generator used to generate the levels.
     */
    private Random random;
    /**
     * < @brief A uniform random genereator.
     */
    private TagDictionary tagDictionary;
    /**
     * < @brief The tag dictionary used to create the flashmob tags.
     */
    private HashMap<Integer, ArrayList<FlashmobTag>> flashmobTags;
    /**
     * < @brief A map of identifiers of tags to flashmob tag instances.
     */
    private FlashmobTag[] flashmobTagCumDist;
    /**
     * < @brief The cumulative distribution of flashmob tags sorted by date.
     */
    private double flashmobTagsPerMonth;
    /**
     * < @brief The number of flashmob tags per month.
     */
    private double probInterestFlashmobTag;
    /**
     * < @brief The probability to take an interest flashmob tag.
     */
    private double probRandomPerLevel;

    /**
     * < @brief The probability per level to take a flashmob tag.
     */

    public FlashmobTagDictionary(TagDictionary tagDictionary,
                                 DateGenerator dateGen,
                                 int flashmobTagsPerMonth,
                                 double probInterestFlashmobTag,
                                 double probRandomPerLevel,
                                 double flashmobTagMinLevel,
                                 double flashmobTagMaxLevel,
                                 double flashmobTagDistExp) {

        this.tagDictionary = tagDictionary;
        this.dateGen = dateGen;
        this.levelGenerator = new PowerDistGenerator(flashmobTagMinLevel, flashmobTagMaxLevel, flashmobTagDistExp);
        this.random = new Random(0);
        this.flashmobTags = new HashMap<Integer, ArrayList<FlashmobTag>>();
        this.flashmobTagsPerMonth = flashmobTagsPerMonth;
        this.probInterestFlashmobTag = probInterestFlashmobTag;
        this.probRandomPerLevel = probRandomPerLevel;
		initialize();
    }

    /**
     * @brief Initializes the flashmob tag dictionary, by selecting a set of tags as flashmob tags.
     */
    private void initialize() {
        int numFlashmobTags = (int) (flashmobTagsPerMonth * dateGen.numberOfMonths(dateGen.getStartDateTime()));
        Integer[] tags = tagDictionary.getRandomTags(random, numFlashmobTags);
        flashmobTagCumDist = new FlashmobTag[numFlashmobTags];
        double sumLevels = 0;
        for (int i = 0; i < numFlashmobTags; ++i) {
            ArrayList<FlashmobTag> instances = flashmobTags.get(tags[i]);
            if (instances == null) {
                instances = new ArrayList<FlashmobTag>();
                flashmobTags.put(tags[i], instances);
            }
            FlashmobTag flashmobTag = new FlashmobTag();
            flashmobTag.date = dateGen.randomDate(random, dateGen.getStartDateTime());
            flashmobTag.level = levelGenerator.getValue(random);
            sumLevels += flashmobTag.level;
            flashmobTag.tag = tags[i];
            instances.add(flashmobTag);
            flashmobTagCumDist[i] = flashmobTag;
//            if(tags[i] == 1761) System.out.println(flashmobTag);
        }
        Arrays.sort(flashmobTagCumDist);
        int size = flashmobTagCumDist.length;
        double currentProb = 0.0;
        for (int i = 0; i < size; ++i) {
            flashmobTagCumDist[i].prob = currentProb;
            currentProb += (double) (flashmobTagCumDist[i].level) / (double) (sumLevels);
        }
    }

    /**
     * @return The index to the earliest flashmob tag.
     * @brief Selects the earliest flashmob tag index from a given date.
     * @param[in] fromDate The minimum date to consider.
     */
    private int searchEarliestIndex(long fromDate) {
        int lowerBound = 0;
        int upperBound = flashmobTagCumDist.length;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (flashmobTagCumDist[midPoint].date > fromDate) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    /**
     * @return true if the flashmob tag is selected. false otherwise.
     * @brief Makes a decision of selecting or not a flashmob tag.
     * @param[in] index The index of the flashmob tag to select.
     */
    private boolean selectFlashmobTag(Random rand, int index) {
        return rand.nextDouble() > (1 - probRandomPerLevel * flashmobTagCumDist[index].level);
    }

    /**
     * @return A vector containing the selected flashmob tags.
     * @brief Given a set of interests and a date, generates a set of flashmob tags.
     * @param[in] interests The set of interests.
     * @param[in] fromDate The date from which to consider the flashmob tags.
     */
    public ArrayList<FlashmobTag> generateFlashmobTags(Random rand, TreeSet<Integer> interests, long fromDate) {
        ArrayList<FlashmobTag> result = new ArrayList<FlashmobTag>();
        Iterator<Integer> it = interests.iterator();
        while (it.hasNext()) {
            Integer tag = it.next();
            ArrayList<FlashmobTag> instances = flashmobTags.get(tag);
            if (instances != null) {
                Iterator<FlashmobTag> it2 = instances.iterator();
                while (it2.hasNext()) {
                    FlashmobTag instance = it2.next();
                    if (instance.date >= fromDate) {
                        if (rand.nextDouble() > 1 - probInterestFlashmobTag) {
                            result.add(instance);
                        }
                    }
                }
            }
        }
        int earliestIndex = searchEarliestIndex(fromDate);
        for (int i = earliestIndex; i < flashmobTagCumDist.length; ++i) {
            if (selectFlashmobTag(rand,i)) {
                result.add(flashmobTagCumDist[i]);
            }
        }
        return result;
    }

    public FlashmobTag[] getFlashmobTags() {
        return flashmobTagCumDist;
    }
}

/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.TagDictionary;
import ldbc.snb.datagen.objects.Person.PersonSummary;
import ldbc.snb.datagen.util.DistributionKey;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;


public class TweetGenerator extends TextGenerator {
    private DistributionKey hashtag;
    private DistributionKey sentiment;
    private DistributionKey popularword;
    //distribution popular,negative, neutral tweets
    private DistributionKey lengthsentence; // sentece length and sentences per tweet
    private DistributionKey lengthtweet;

    public TweetGenerator(Random random, TagDictionary tagDic) throws NumberFormatException, IOException {
        super(random, tagDic);
        //input de fitxers i crea els 4 maps
        hashtag = new DistributionKey(DatagenParams.SPARKBENCH_DIRECTORY + "/hashtags.csv");
        sentiment = new DistributionKey(DatagenParams.SPARKBENCH_DIRECTORY + "/sentiment.csv");
        popularword = new DistributionKey(DatagenParams.SPARKBENCH_DIRECTORY + "/words.csv");
        //proportion = new DistributionKey(DatagenParams.SPARKBENCH_DIRECTORY + "/sentiment.csv");
        lengthsentence = new DistributionKey(DatagenParams.SPARKBENCH_DIRECTORY + "/sentence_lengths.csv");
        lengthtweet = new DistributionKey(DatagenParams.SPARKBENCH_DIRECTORY + "/sentence_count.csv");
    }

    @Override
    protected void load() {
        // Intentionally left empty
    }

    @Override
    public String generateText(PersonSummary member, TreeSet<Integer> tags, Properties prop) {
        StringBuffer content = null;
        //mirar num de frases
        Double numsentences = Double.valueOf(lengthtweet.nextDouble(this.random));
        for (int i = 0; i < numsentences; ++i) {
            Double numwords = Double.valueOf(lengthsentence.nextDouble(this.random));
            // depenen de la distribució de number hashtags per sentence int numhashtags;
            //int numhashtags = funciondistribuciohashtags(numwords);
            int numhashtags = (int) (numwords * 0.4);
            for (int j = 0; j < numhashtags; ++j) {
                content.append(" " + hashtag.nextDouble(this.random));
            }
            // depenen de la distribució de number sentiment words per sentence int numhashtags;
            //int numsentimentswords = funciondistribuciosentimentswords(numwords);
            int numsentimentswords = (int) (numwords * 0.4);
            for (int q = 0; q < numhashtags; ++q) {
                content.append(" " + sentiment.nextDouble(this.random));
            }
            numwords -= (numsentimentswords + numhashtags);
            for (int j = 0; j < numwords; ++j) {
                content.append(" " + popularword.nextDouble(this.random));
            }

        }
        //per cada frase mirar numero de paraules
        //mirar numero de hashtags
        content.toString();
        return content.toString();

    }


}

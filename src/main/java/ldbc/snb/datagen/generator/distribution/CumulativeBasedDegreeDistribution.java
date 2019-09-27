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
package ldbc.snb.datagen.generator.distribution;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by aprat on 12/05/15.
 */
public abstract class CumulativeBasedDegreeDistribution extends DegreeDistribution {

    private ArrayList<CumulativeEntry> cumulativeProbability_;
    private Random random_;

    public class CumulativeEntry {
        public double prob_;
        public int value_;
    }

    public void initialize(Configuration conf) {
        cumulativeProbability_ = cumulativeProbability(conf);
        random_ = new Random();
    }

    public void reset(long seed) {
        random_.setSeed(seed);
    }

    public long nextDegree() {
        double prob = random_.nextDouble();
        int index = binarySearch(cumulativeProbability_, prob);
        return cumulativeProbability_.get(index).value_;
    }

    private int binarySearch(ArrayList<CumulativeEntry> cumulative, double prob) {
        int upperBound = cumulative.size() - 1;
        int lowerBound = 0;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound > (lowerBound + 1)) {
            if (cumulative.get(midPoint).prob_ > prob) {
                upperBound = midPoint;
            } else {
                lowerBound = midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    public abstract ArrayList<CumulativeEntry> cumulativeProbability(Configuration conf);
}

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

import ldbc.snb.datagen.generator.DatagenParams;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;

/**
 * Created by aprat on 26/02/15.
 */
public class AltmannDistribution extends CumulativeBasedDegreeDistribution {

    private double normalization_factor_ = 0.0;
    private double ALPHA_ = 0.4577;
    private double BETA_ = 0.0162;


    public ArrayList<CumulativeEntry> cumulativeProbability(Configuration conf) {
        //throw new UnsupportedOperationException("Distribution not implemented");
        ALPHA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.AltmannDistribution.alpha", ALPHA_);
        BETA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.AltmannDistribution.beta", BETA_);

        long POPULATION_ = DatagenParams.numPersons;
        for (int i = 1; i <= POPULATION_; ++i) {
            normalization_factor_ += Math.pow(i, -ALPHA_) * Math.exp(-BETA_ * i);
        }
        ArrayList<CumulativeEntry> cumulative = new ArrayList<CumulativeEntry>();
        for (int i = 1; i <= POPULATION_; ++i) {
            double prob = Math.pow(i, -ALPHA_) * Math.exp(-BETA_ * i) / normalization_factor_;
            prob += cumulative.size() > 0 ? cumulative.get(i - 2).prob_ : 0.0;
            CumulativeEntry entry = new CumulativeEntry();
            entry.prob_ = prob;
            entry.value_ = i;
            cumulative.add(entry);
        }
        return cumulative;
    }
}

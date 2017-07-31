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
 * Created by aprat on 5/03/15.
 */
public class DiscreteWeibullDistribution extends CumulativeBasedDegreeDistribution {

    //private double BETA_ = 0.7787;
    //private double BETA_ = 0.8211;
    private double BETA_ = 0.8505;
    //private double P_ = 0.062;
    //private double P_ = 0.0448;
    private double P_ = 0.0205;

    public ArrayList<CumulativeEntry> cumulativeProbability(Configuration conf) {
        BETA_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.DiscreteWeibullDistribution.beta", BETA_);
        P_ = conf.getDouble("ldbc.snb.datagen.generator.distribution.DiscreteWeibullDistribution.p", P_);
        ArrayList<CumulativeEntry> cumulative = new ArrayList<CumulativeEntry>();
        for (int i = 0; i < DatagenParams.numPersons; ++i) {
            //double prob = Math.pow(1.0-P_,Math.pow(i,BETA_))-Math.pow((1.0-P_),Math.pow(i+1,BETA_));
            double prob = 1.0 - Math.pow((1.0 - P_), Math.pow(i + 1, BETA_));
            CumulativeEntry entry = new CumulativeEntry();
            entry.prob_ = prob;
            entry.value_ = i + 1;
            cumulative.add(entry);
        }
        return cumulative;
    }
}

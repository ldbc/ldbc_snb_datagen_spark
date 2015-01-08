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

import ldbc.snb.datagen.generator.DatagenParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

public class BrowserDictionary {

    private static final String SEPARATOR_ = "  ";
    private ArrayList<String> browsers_;
    private ArrayList<Double> cumulativeDistribution_;
    private double probAnotherBrowser_ = 0.0f;                   

    public BrowserDictionary(double probAnotherBrowser) {
        probAnotherBrowser_ = probAnotherBrowser;
        browsers_ = new ArrayList<String>();
        cumulativeDistribution_ = new ArrayList<Double>();
	load(DatagenParams.browserDictonryFile);
    }

    private void load(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            double cummulativeDist = 0.0;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR_);
                String browser = data[0];
                cummulativeDist += Double.parseDouble(data[1]);
                browsers_.add(browser);
                cumulativeDistribution_.add(cummulativeDist);
            }
            dictionary.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getName(int id) {
        return browsers_.get(id);
    }

    public int getRandomBrowserId(Random random) {
        double prob = random.nextDouble();
        int minIdx = 0;
        int maxIdx = (byte) ((prob < cumulativeDistribution_.get(minIdx)) ? minIdx : cumulativeDistribution_.size() - 1);
        // Binary search
        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution_.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return maxIdx;
    }

    public int getPostBrowserId(Random randomDiffBrowser, Random randomBrowser, int userBrowserId) {
        double prob = randomDiffBrowser.nextDouble();
        return (prob < probAnotherBrowser_) ? getRandomBrowserId(randomBrowser) : userBrowserId;
    }
}

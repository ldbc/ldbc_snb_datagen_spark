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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;

/**
 * This class reads the file containing the names and distributions for the browsers used in the ldbc socialnet generation and
 * provides access methods to get such data.
 */
public class BrowserDictionary {

    private static final String SEPARATOR = "  ";
    private ArrayList<String> browsers;
    /**
     * < @brief The browsers in the dictionary.*
     */
    private ArrayList<Double> cumulativeDistribution;
    /**
     * < @brief The cumulative distribution of each browser.
     */
    private double probAnotherBrowser = 0.0f;                   /**< @brief The probability that a user uses another browser than its initial one.*/

    /**
     * @param probAnotherBrowser: Probability of the user using another browser.
     * @brief Constructor.
     */
    public BrowserDictionary(double probAnotherBrowser) {
        this.probAnotherBrowser = probAnotherBrowser;
        this.browsers = new ArrayList<String>();
        this.cumulativeDistribution = new ArrayList<Double>();
    }

    /**
     * @brief Initializes the dictionary extracting the data from the file.
     */
    public void load(String fileName) {
        try {
            BufferedReader dictionary = new BufferedReader(
                    new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            double cummulativeDist = 0.0;
            while ((line = dictionary.readLine()) != null) {
                String data[] = line.split(SEPARATOR);
                String browser = data[0];
                cummulativeDist += Double.parseDouble(data[1]);
                browsers.add(browser);
                cumulativeDistribution.add(cummulativeDist);
            }
            dictionary.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param id The id of the browser to get its name.
     * @brief Gets the browser name.
     */
    public String getName(int id) {
        return browsers.get(id);
    }

    /**
     * @brief Gets a random browser id.
     * @brief random The random number generator to use.
     */
    public int getRandomBrowserId(Random random) {
        double prob = random.nextDouble();
        int minIdx = 0;
        int maxIdx = (byte) ((prob < cumulativeDistribution.get(minIdx)) ? minIdx : cumulativeDistribution.size() - 1);
        // Binary search
        while ((maxIdx - minIdx) > 1) {
            int middlePoint = minIdx + (maxIdx - minIdx) / 2;
            if (prob > cumulativeDistribution.get(middlePoint)) {
                minIdx = middlePoint;
            } else {
                maxIdx = middlePoint;
            }
        }
        return maxIdx;
    }

    /**
     * @param userBrowserId: The user preferred browser.
     * @brief Gets the post browser. There is a chance of being different from the user preferred browser
     */
    public int getPostBrowserId(Random randomDiffBrowser, Random randomBrowser, int userBrowserId) {
        double prob = randomDiffBrowser.nextDouble();
        return (prob < probAnotherBrowser) ? getRandomBrowserId(randomBrowser) : userBrowserId;
    }
}

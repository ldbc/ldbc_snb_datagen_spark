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


public class UserAgentDictionary {

    private ArrayList<String> userAgents;
    /**
     * < @brief The set of all user agents.
     */
    private double probSentFromAgent;      /**< @brief The probability to used a different agent.*/

    /**
     * @param probSentFromAgent The probability to use a different agent.
     * @brief Constructor
     */
    public UserAgentDictionary(double probSentFromAgent) {
        this.probSentFromAgent = probSentFromAgent;
	load(DatagenParams.agentFile);
    }

    /**
     * @param fileName The agent dictionary file name.
     * @brief Loads an agent dictionary file.
     */
    private void load(String fileName) {
        try {
            userAgents = new ArrayList<String>();
            BufferedReader agentFile = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
            String line;
            while ((line = agentFile.readLine()) != null) {
                userAgents.add(line.trim());
            }
            agentFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param randomSent    The random number generator.
     * @param hasSmathPhone Tells if we want an smartphone.
     * @param agentId       The user agent id.
     * @return The user agent name.
     * @brief Get a user agen name.
     */
    public String getUserAgentName(Random randomSent, boolean hasSmathPhone, int agentId) {
        return (hasSmathPhone && (randomSent.nextDouble() > probSentFromAgent)) ? userAgents.get(agentId) : "";
    }

    /**
     * @param random The random number generator used.
     * @return The user agent id.
     * @brief Gets a random user agent.
     */
    public int getRandomUserAgentIdx(Random random) {
        return random.nextInt(userAgents.size());
    }
}

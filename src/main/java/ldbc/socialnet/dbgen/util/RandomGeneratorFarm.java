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

package ldbc.socialnet.dbgen.util;

import java.util.Random;

public class RandomGeneratorFarm {

    int           numRandomGenerators;
    Random[]            randomGenerators;

    public enum Aspect {
        DATE,
        BIRTH_DAY,
        FRIEND_REQUEST,
        FRIEND_REJECT,
        FRIEND_APROVAL,
        INITIATOR,
        UNIFORM,
        NUM_INTEREST,
        NUM_TAG,
        NUM_FRIEND,
        NUM_COMMENT,
        NUM_PHOTO_ALBUM,
        NUM_PHOTO,
        NUM_GROUP,
        NUM_USERS_PER_GROUP,
        NUM_POPULAR,
        NUM_LIKE,
        NUM_POST,
        FRIEND,
        FRIEND_LEVEL,
        GENDER,
        RANDOM,
        MEMBERSHIP,
        MEMBERSHIP_INDEX,
        GROUP,
        GROUP_MODERATOR,
        GROUP_INTEREST,
        EXTRA_INFO,
        EXACT_LONG_LAT,
        STATUS,
        HAVE_STATUS,
        STATUS_SINGLE,
        USER_AGENT,
        USER_AGENT_SENT,
        FILE_SELECT,
        IP,
        DIFF_IP_FOR_TRAVELER,
        DIFF_IP,
        BROWSER,
        DIFF_BROWSER,
        CITY,
        COUNTRY,
        TAG,
        UNIVERSITY,
        UNCORRELATED_UNIVERSITY,
        UNCORRELATED_UNIVERSITY_LOCATION,
        TOP_UNIVERSITY,
        POPULAR,
        EMAIL,
        TOP_EMAIL,
        COMPANY,
        UNCORRELATED_COMPANY,
        UNCORRELATED_COMPANY_LOCATION,
        LANGUAGE,
        ALBUM,
        ALBUM_MEMBERSHIP,
        NAME,
        SURNAME,
        TAG_OTHER_COUNTRY,
        SET_OF_TAG,
        TEXT_SIZE,
        REDUCED_TEXT,
        LARGE_TEXT,
        MEMBERSHIP_POST_CREATOR,
        REPLY_TO,
        TOPIC,
        NUM_ASPECT                  // This must be always the last one.
    }

    public RandomGeneratorFarm () {
        numRandomGenerators = Aspect.values().length;
        randomGenerators = new Random[numRandomGenerators];
        for( int i = 0; i < numRandomGenerators; ++i) {
            randomGenerators[i] = new Random();
        }
    }

    public Random get(Aspect aspect) {
        return randomGenerators[aspect.ordinal()];
    }

    public void resetRandomGenerators( long seed ) {
        Random seedRandom = new Random(53223436L + 1234567*seed);
        for (int i = 0; i < numRandomGenerators; i++) {
            randomGenerators[i].setSeed(seedRandom.nextLong());
        }
    }
}

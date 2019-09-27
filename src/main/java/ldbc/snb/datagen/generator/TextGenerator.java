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
import ldbc.snb.datagen.objects.Person;

import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

public abstract class TextGenerator {
    protected TagDictionary tagDic;
    protected Random random;


    /**
     * < @brief The probability to retrieve an small text.
     */

    public TextGenerator(Random random, TagDictionary tagDic) {
        this.tagDic = tagDic;
        this.random = random;
    }

    /**
     * @param fileName The tag text dictionary file name.
     * @brief Loads the dictionary.
     */
    protected abstract void load();

    /**
     * @param randomTextSize The random number generator to generate the amount of text devoted to each tag.
     * @param tags           The set of tags to generate the text from.
     * @param textSize       The final text size.
     * @return The final text.
     * @brief Generates a text given a set of tags.
     */
    //info extra
    public abstract String generateText(Person.PersonSummary person, TreeSet<Integer> tags, Properties prop);

}

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
package ldbc.snb.datagen.generator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.Vector;

public class RandomNamesGenerator {
    String dicFileName;
    Vector<String> vecNames;        // vector for storing all the names
    RandomAccessFile dictionary;
    Random rand;

    public RandomNamesGenerator(String fileName) {
        rand = new Random();
        dicFileName = fileName;
        init();
    }

    public RandomNamesGenerator(String fileName, long seed) {
        dicFileName = fileName;
        rand = new Random(seed);
        init();
    }

    public void init() {
        try {
            dictionary = new RandomAccessFile(dicFileName, "r");
            vecNames = new Vector<String>();

            System.out.println("Extracting names into a dictionary ");
            extractNames();

            dictionary.close();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void extractNames() {
        String name;
        try {
            while ((name = dictionary.readLine()) != null) {
                vecNames.add(name);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println(vecNames.size() + " names were extracted");

    }

    public String getRandomFirstName() {
        int idx = rand.nextInt(vecNames.size() - 1);
        return vecNames.elementAt(idx);
    }

    public String getRandomLastName() {
        int idx = rand.nextInt(vecNames.size() - 1);
        return vecNames.elementAt(idx);
    }

}


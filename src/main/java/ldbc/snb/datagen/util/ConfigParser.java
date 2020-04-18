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
package ldbc.snb.datagen.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigParser {

    public static Map<String, String> readConfig(String paramsFile) {
        try {
            return readConfig(new FileInputStream(paramsFile));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> readConfig(InputStream paramStream) {
        Map<String, String> conf = new HashMap<>();
        try {
            ScaleFactors scaleFactors = ScaleFactors.INSTANCE;
            Properties properties = new Properties();
            properties.load(new InputStreamReader(paramStream, StandardCharsets.UTF_8));
            String val = (String) properties.get("ldbc.snb.datagen.generator.scaleFactor");
            if (val != null) {
                if (!scaleFactors.value.containsKey(val)) {
                    throw new IllegalArgumentException("Scale factor " + val + " does not exist");
                }
                ScaleFactor scaleFactor = scaleFactors.value.get(val);
                System.out.println("Applied configuration of scale factor " + val);
                for (Map.Entry<String, String> e : scaleFactor.properties.entrySet()) {
                    conf.put(e.getKey(), e.getValue());
                }
            }

            for (String s : properties.stringPropertyNames()) {
                if (s.compareTo("ldbc.snb.datagen.generator.scaleFactor") != 0) {
                    conf.put(s, properties.getProperty(s));
                }
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
        return conf;
    }

    public static void printConfig(Map<String, String> conf) {
        System.out.println("********* Configuration *********");
        conf.forEach((key, value) -> System.out.println(key + ": " + value));
        System.out.println("*********************************");
    }

    public static Map<String, String> defaultConfiguration() {
        Map<String, String> conf = new HashMap<>();
        conf.put("", Integer.toString(1));
        conf.put("ldbc.snb.datagen.generator.numPersons", "10000");
        conf.put("ldbc.snb.datagen.generator.startYear", "2010");
        conf.put("ldbc.snb.datagen.generator.numYears", "3");
        conf.put("ldbc.snb.datagen.generator.numThreads", Integer.toString(1));
        conf.put("ldbc.snb.datagen.generator.deltaTime", "10000");
        conf.put("ldbc.snb.datagen.generator.distribution.degreeDistribution", "ldbc.snb.datagen.generator.distribution.FacebookDegreeDistribution");
        conf.put("ldbc.snb.datagen.generator.knowsGenerator", "ldbc.snb.datagen.generator.generators.knowsgenerators.DistanceKnowsGenerator");
        conf.put("ldbc.snb.datagen.generator.person.similarity", "ldbc.snb.datagen.entities.dynamic.person.similarity.GeoDistanceSimilarity");
        conf.put("serializer.format","CsvBasic"); // CsvBasic, CsvMergeForeign, CsvComposite, CsvCompositeMergeForeign
        conf.put("serializer.compressed", "false");
        conf.put("ldbc.snb.datagen.serializer.outputDir", "./");
        conf.put("ldbc.snb.datagen.serializer.socialNetworkDir", "./social_network");
        conf.put("ldbc.snb.datagen.serializer.hadoopDir", "./hadoop");
        conf.put("ldbc.snb.datagen.serializer.endlineSeparator", Boolean.toString(false));
        conf.put("ldbc.snb.datagen.serializer.dateFormatter", "ldbc.snb.datagen.util.formatter.StringDateFormatter");
        conf.put("ldbc.snb.datagen.util.formatter.StringDateFormatter.dateTimeFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");
        conf.put("ldbc.snb.datagen.util.formatter.StringDateFormatter.dateFormat", "yyyy-MM-dd");
        conf.put("ldbc.snb.datagen.parametergenerator.python", "python");
        conf.put("ldbc.snb.datagen.parametergenerator.parameters", "true");
        conf.put("ldbc.snb.datagen.mode", "interactive"); // interactive, bi, graphalytics, rawdata
        conf.put("ldbc.snb.datagen.mode.bi.deleteType", "simple"); // simple or smart
        conf.put("ldbc.snb.datagen.mode.bi.batches", "1");
        conf.put("ldbc.snb.datagen.mode.interactive.numUpdateStreams", "1");

        return conf;
    }
}

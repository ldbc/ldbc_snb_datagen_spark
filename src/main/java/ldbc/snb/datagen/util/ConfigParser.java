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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConfigParser {

    public static Map<String, String> readConfig(String paramsFile) {
        try(FileInputStream fis = new FileInputStream(paramsFile)) {
            return readConfig(fis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> readConfig(Properties properties) {
        Map<String, String> conf = new HashMap<>();
        ScaleFactors scaleFactors = ScaleFactors.INSTANCE;
        String val = (String) properties.get("generator.scaleFactor");
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
            if (s.compareTo("generator.scaleFactor") != 0) {
                conf.put(s, properties.getProperty(s));
            }
        }
        return conf;
    }

    public static Map<String, String> readConfig(InputStream paramStream) {
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(paramStream, StandardCharsets.UTF_8));
            return readConfig(properties);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> defaultConfiguration() {
        Map<String, String> conf = new HashMap<>();
        conf.put("", Integer.toString(1));
        conf.put("generator.numPersons", "10000");
        conf.put("generator.startYear", "2010");
        conf.put("generator.numYears", "3");
        conf.put("generator.delta", "10000");
        conf.put("generator.mode", "interactive");
        conf.put("generator.mode.bi.deleteType", "simple");
        conf.put("generator.mode.bi.batches", "1");
        conf.put("generator.mode.interactive.numUpdateStreams", "1");
        conf.put("generator.degreeDistribution", "Facebook");
        conf.put("generator.knowsGenerator", "Distance");
        conf.put("generator.person.similarity", "GeoDistance");
        conf.put("generator.dateFormatter", "StringDate");
        conf.put("generator.StringDate.dateTimeFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");
        conf.put("generator.StringDate.dateFormat", "yyyy-MM-dd");

        conf.put("hadoop.numThreads", "1");
        conf.put("serializer.format","CsvBasic");
        conf.put("serializer.compressed", "false");
        conf.put("serializer.insertTrailingSeparator", "false");
        conf.put("serializer.socialNetworkDir", "./social_network");
        conf.put("serializer.buildDir", "./build");

        return conf;
    }

}

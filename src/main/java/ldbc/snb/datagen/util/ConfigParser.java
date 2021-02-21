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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    public static Map<String, String> scaleFactorConf(String scaleFactorId) {
        Map<String, String> conf = new HashMap<>();
        ScaleFactors scaleFactors = ScaleFactors.INSTANCE;
        if (!scaleFactors.value.containsKey(scaleFactorId)) {
            throw new IllegalArgumentException("Scale factor " + scaleFactorId + " does not exist");
        }
        ScaleFactor scaleFactor = scaleFactors.value.get(scaleFactorId);
        System.out.println("Applied configuration of scale factor " + scaleFactorId);
        for (Map.Entry<String, String> e : scaleFactor.properties.entrySet()) {
            conf.put(e.getKey(), e.getValue());
        }
        return conf;
    }

    public static Map<String, String> readConfig(InputStream paramStream) {
        Properties properties = new Properties();
        Map<String, String> res = new HashMap<>();
        try {
            properties.load(new InputStreamReader(paramStream, StandardCharsets.UTF_8));
            for (String s: properties.stringPropertyNames()) {
                res.put(s, properties.getProperty(s));
            }
            return res;
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}

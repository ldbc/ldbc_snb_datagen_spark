/*
 *  Big Database Semantic Metric Tools
 *
 * Copyright (C) 2011-2013 OpenLink Software <bdsmt@openlinksw.com>
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
package com.openlinksw.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.openlinksw.bibm.Exceptions.ExceptionException;

public class FileUtil {

    public  static void strings2file(File file, String... strings) {
        try {
            FileWriter resultWriter = new FileWriter(file);
            for (String string: strings) {
                resultWriter.append(string);
            }
            resultWriter.flush();
            resultWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static  String file2string(File file) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            StringBuffer sb = new StringBuffer();

            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                else {
                    sb.append(line);
                    sb.append("\n");
                }
            }
            return sb.toString();
        } catch (IOException e) {
            throw new ExceptionException("Error openening or reading file: " + file.getAbsolutePath(), e);
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

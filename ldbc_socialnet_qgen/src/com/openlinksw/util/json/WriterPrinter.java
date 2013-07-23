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
package com.openlinksw.util.json;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import com.openlinksw.bibm.Exceptions.ExceptionException;

public class WriterPrinter  extends Printer {
    protected File file;
    protected Writer wr;
    
    public WriterPrinter(Writer wr) {
        this.wr=wr;
    }

    public WriterPrinter(File file) throws IOException {
        this.file=file;
        this.wr=new FileWriter(file);
    }

    @Override
    public Printer append(char c) {
        try {
            wr.append(c);
        } catch (IOException e) {
           throw new ExceptionException(null, e);
        }
        return this;
    }

    @Override
    public Printer append(String s) {
        try {
            wr.append(s.toString());
        } catch (IOException e) {
            throw new ExceptionException(null, e);
        }
        return this;
    }

    public void flush() throws IOException {
        wr.flush();
    }

}

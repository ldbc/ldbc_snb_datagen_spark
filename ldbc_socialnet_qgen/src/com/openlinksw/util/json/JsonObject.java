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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public abstract class JsonObject  extends JsonBase  {
    protected boolean ignoreCase=false;

    public JsonObject(boolean ignoreCase) {
        this.ignoreCase = ignoreCase;
    }

    public JsonObject() {
        this(false);
    }

    public void loadFrom(BufferedReader in, String filename) throws IOException {
        JsonParser parser = new JsonParser(in, filename);
        JsonStack stack = new JsonStack(this, ignoreCase);
        parser.parseTo(stack);
      }
       
    public void loadFrom(File file) throws IOException {
        loadFrom(new BufferedReader(new FileReader(file)), file.getCanonicalPath());
   }

    public void loadFrom(String fileName) throws IOException {
        loadFrom(new BufferedReader(new FileReader(fileName)), fileName);
   }


    public boolean hasAttr(String key) {
        throw new UnsupportedOperationException("hasAttr");
    }

    public void put(String key, Object value) {
        throw new UnsupportedOperationException("put");
        
    }
    public Object get(String key) {
        throw new UnsupportedOperationException("get");
    }

    /**
     * @return  keys of fields in the order of printing
     */
    public Iterable<String> printOrder() {
        throw new UnsupportedOperationException("printOrder");
    }

    public JsonList<?> newJsonList(String key) {
        throw new ParserException("List not expected here");
    }

    public JsonObject newJsonObject(String key, boolean ignoreCase) {
       throw new ParserException("Object not expected here");
    }

}

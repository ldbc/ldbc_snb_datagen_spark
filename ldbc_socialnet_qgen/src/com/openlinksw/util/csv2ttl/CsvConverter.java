package com.openlinksw.util.csv2ttl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.csvreader.CsvReader;

public class CsvConverter {
    private static final int MAX_FILE_BUFFER_SIZE = 64*1024;
    CsvReader reader;
    CsvTableSchema tableSchema;
    int fieldsSize;
    
    public CsvConverter(File src, char delim, DBSchema dbSchema, CsvTableSchema tableSchema) throws IOException {
        BufferedReader freader = new BufferedReader(new FileReader(src) , MAX_FILE_BUFFER_SIZE);
        reader=new CsvReader(freader, delim);
        this.tableSchema=tableSchema;
        fieldsSize = tableSchema.getFields().size();
    }

    public String getNodeString() throws IOException {
        String[] values = reader.getValues();
        if (values.length<fieldsSize) {
            return null;
        }
        return tableSchema.makeNodeString(values);
    }

    public boolean readRecord() throws IOException {
        return reader.readRecord();
    }
    
}

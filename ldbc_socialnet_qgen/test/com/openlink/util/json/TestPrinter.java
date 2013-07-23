 package com.openlink.util.json;
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

public class TestPrinter {
    @Test
    public void testList() throws IOException {
        Printer printer=new OutPrinter();
        BufferedReader in = new BufferedReader(new FileReader("com/openlink/util/json/list.json"));
        JsonParser parser=new JsonParser(in, printer);
        parser.parse();
    }

}

package com.openlink.util.json;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;

import com.openlink.bsbm.Exceptions.ExceptionException;

public class OutPrinter  extends Printer {
    PrintStream out=System.out;
    
    public OutPrinter() {
        super(false); // FIXME
    }

    @Override
    public Printer append(char c) {
        out.append(c);
        out.flush();
        return this;
    }

    @Override
    public Printer append(Object s) {
        out.append(s.toString());
        out.flush();
       return this;
    }
    
}

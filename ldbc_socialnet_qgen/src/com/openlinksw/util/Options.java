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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.openlinksw.bibm.Exceptions.BadSetupException;

public class Options {
    protected String[] usageHeader;
    protected TreeMap<String, Option<?>> options=new TreeMap<String, Option<?>>();
    protected ArrayList<String> args=new ArrayList<String>();
    
    public Options(String... usageHeader) {
        this.usageHeader = usageHeader;
    }

    protected void printUsageInfos() {
        StringBuilder sb=new StringBuilder("\n");
        for (String line: usageHeader) {
            sb.append(line).append('\n');
        }
        System.out.println("options:");
        for (String optName: options.keySet()) {
            Option<?> option = options.get(optName);
            option.printHelp(sb);
        }
        System.out.println(sb.toString());
    }

    protected void processProgramParameters(String[] args) {
        DoubleLogger err = DoubleLogger.getErr();
        int i = 0;
        while (i < args.length) {
            try {
                String arg = args[i++];
                String optionName;
                if (arg.startsWith("-")) {
                     optionName=arg.substring(1);
                } else {
                    this.args.add(arg);
                    continue;
                }
                Option<?> opt=options.get(optionName);
                if (opt==null) {
                    err.println("Unknown parameter: ", arg);
                    printUsageInfos();
                    System.exit(-1);
                }
                if (opt instanceof BooleanOption) {
                    ((BooleanOption)opt).setValue(true);
                    continue;
                }
                if (i==args.length) {
                    err.println("Invalid arguments: option ", optionName, " needs value");
                    printUsageInfos();
                    System.exit(-1);
                }
                String arg2=args[i++];
                if (opt instanceof IntegerOption) {
                    ((IntegerOption)opt).setValue(Integer.parseInt(arg2));
                } else  if (opt instanceof LongOption) {
                    ((LongOption)opt).setValue(Long.parseLong(arg2));
                } else  if (opt instanceof DoubleOption) {
                    ((DoubleOption)opt).setValue(Double.parseDouble(arg2));
                } else  if (opt instanceof FileOption) {
                    ((FileOption)opt).setValue(new File(arg2));
                } else  if (opt instanceof StringOption) {
                    ((StringOption)opt).setValue(arg2);
                } else  if (opt instanceof MultiStringOption) {
                    ((MultiStringOption)opt).addValue(arg2);
                } else {
                    throw new BadSetupException("Unknown parameter type for: " + arg+" :"+opt.getClass().getName());
                }
            } catch (Exception e) {
                err.println("Invalid arguments:"+e.toString());
                printUsageInfos();
                System.exit(-1);
            }
        }
    }
        
    public class Option<ValueType> {
        String name;
        String nameWithComment;
        String[] helpStrings;
        ValueType defaultValue;
        ValueType setValue;
        
        public Option(String nameWithComment, ValueType defaultValue, String... helpStrings) {
            this.name = nameWithComment.split("\\s+")[0]; // the first word
            this.nameWithComment = nameWithComment;
            options.put(name, this);
            this.defaultValue=defaultValue;
            this.helpStrings = helpStrings;
        }

        public ValueType getSetValue() {
            return setValue;
        }

        public ValueType getValue() {
            if (setValue!=null) {
                return setValue;
            } else {
                return defaultValue;                
            }
        }

        public void setValue(ValueType value) {
            this.setValue = value;
        }
        
        void printHelp(StringBuilder sb) {
            sb.append("\t-").append(nameWithComment).append('\n');
            for (String line: helpStrings) {
                // substitute %% with the default value
                Object defaultValue = this.defaultValue;
                if (defaultValue==null) {
                    defaultValue="none";
                }
                line=line.replace("%%", defaultValue.toString()); 
                sb.append("\t\t").append(line).append('\n');
            }
        }

    }
    
    /**
     * an option without argument 
     * @author ak
     *
     */
    public class BooleanOption extends Option<Boolean> {
        public BooleanOption(String name, String... helpStrings) {
            super(name, Boolean.FALSE, helpStrings);
        }
    }
    
    /**
     * an option with an integer argument 
     * @author ak
     *
     */
    public class LongOption extends Option<Long> {
        public LongOption(String name, Long defaultValue, String... helpStrings) {
            super(name, defaultValue, helpStrings);
        }
    }
    
    /**
     * an option with an integer argument 
     * @author ak
     *
     */
    public class IntegerOption extends Option<Integer> {
        public IntegerOption(String name, Integer defaultValue, String... helpStrings) {
            super(name, defaultValue, helpStrings);
        }
    }
    
    /**
     * an option with a double argument 
     * @author ak
     *
     */
    public class DoubleOption extends Option<Double> {
        public DoubleOption(String name, Double defaultValue, String... helpStrings) {
            super(name, defaultValue, helpStrings);
        }
    }
    
    /**
     * an option with a String argument 
     * @author ak
     *
     */
    public class StringOption extends Option<String> {
        public StringOption(String name, String defaultValue, String... helpStrings) {
            super(name, defaultValue, helpStrings);
        }
    }
    
    /**
     * an option with a String argument meaning a file name
     * @author ak
     *
     */
    public class FileOption extends Option<File> {
        
        public FileOption(String name, String defaultValue, String... helpStrings) {
            super(name, defaultValue==null?null:new File(defaultValue), helpStrings);
        }

        public File newNumberedFile() {
            File resFile=getValue();
            if (!resFile.exists()) {
                return resFile;
            }
            // construct another file name, with increased number
            String resname=resFile.getName();
            String ext; // with dot
            int k=resname.lastIndexOf('.');
            if (k==-1) { // strange..
                ext="";
            } else {
                ext=resname.substring(k);
                resname=resname.substring(0, k);
            }
            File dir=resFile.getParentFile();
            if (dir==null) {
                dir=new File(".");
            }
            int maxnum=0;
            for (String name: dir.list()) {
                if (!name.startsWith(resname)) continue;
                if (!name.endsWith(ext)) continue;
                String numStr;
                if (ext.length()==0) {
                    numStr=name.substring(resname.length());
                } else {
                    int start = resname.length()+1;
                    int end = name.lastIndexOf(ext);
                    if (start>=end) continue; // no number
                    numStr=name.substring(start, end);                  
                }
                int num;
                try {
                    num=Integer.parseInt(numStr);
                } catch (NumberFormatException e) {
                    continue; //alien file name
                }
                if (num>maxnum) {
                    maxnum=num;
                }
            }
            resname=resname+'.'+(maxnum+1)+ext;
            resFile=new File(resname);
            return resFile;
        }
    }
    
    /**
     * an option with a String argument 
     * options with the same name are collected in a List 
     * no default value supposed 
     * @author ak
     *
     */
    public class MultiStringOption extends Option<List<String>> {
        public MultiStringOption(String name, String... helpStrings) {
            super(name, new ArrayList<String>(), helpStrings);
        }

        protected void addValue(String value) {
            this.defaultValue.add(value);
        }
        
    }
    
}

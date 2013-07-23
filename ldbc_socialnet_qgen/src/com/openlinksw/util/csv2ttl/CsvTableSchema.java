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
package com.openlinksw.util.csv2ttl;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.util.Nameable;
import com.openlinksw.util.csv2ttl.DBSchema.CsvFields;
import com.openlinksw.util.json.JsonList;
import com.openlinksw.util.json.JsonObject;
import com.openlinksw.util.json.ParserException;
import com.openlinksw.util.json.impl.AutoJsonObject;
import com.openlinksw.util.json.impl.IgnoreCase;
import com.openlinksw.util.json.impl.Sequence;
import com.openlinksw.util.json.impl.SimpleJsonList;

@IgnoreCase
@Sequence(value={"name", "tag",  "fields", "keys"})
public class CsvTableSchema extends AutoJsonObject implements Nameable {
    private DBSchema dbSchema;
    private String name;
    private String tag;
    private Fields fields;
    private JsonList<String> keys;
    private int[] keyIndexes; 
	
    public CsvTableSchema(DBSchema dbSchema) {
        this.dbSchema=dbSchema;
    }

    void fixFields(CsvFields commonFields) {
        if (tag==null) {
            tag=dbSchema.default_tag;
        }
        if (keys==null) {
            return;
        }
        keyIndexes=new int[keys.size()];
        for (int k=0; k<keyIndexes.length; k++) {
            keyIndexes[k]=-1;            
        }
        for (int k=0; k<fields.size(); k++) {
            Object fieldOrString=fields.get(k);
            String fieldName;
            CsvField field;
            if (fieldOrString instanceof String) {
                fieldName=(String)fieldOrString;
                field = commonFields.get(fieldName);
                if (field==null) {
                    throw new ParserException("commond fielf not declared:"+fieldName);
                }
                fields.set(k, field);
            } else  if (fieldOrString instanceof CsvField) {
                field=(CsvField)fieldOrString;
                fieldName=field.getName();
            } else {
                throw new BadSetupException("wrong field class:"+fieldOrString.getClass().getName());
            }
            field.fixField(this);
            int keyIndex=keys.indexOf(fieldName);
            if (keyIndex!=-1) { // yes this is a key
                keyIndexes[keyIndex]=k;
            }
        }
        for (int k=0; k<keyIndexes.length; k++) {
            if (keyIndexes[k]==-1) {
                throw new BadSetupException("In table '"+name+"': key '"+keys.get(k)+"' not declared as field");            
            }
        }
    }
    
    public String makeSubjectString(String value) {
        StringBuilder sb=new StringBuilder();
        if (tag!=null) {
            sb.append(tag).append(':');
        }
        sb.append(name).append("_").append(value);
        return sb.toString();
    }
    
    public String makeNodeString(String[] values) {
        JsonList<CsvField> fields = getFields();
        StringBuilder sb=new StringBuilder();

        // construct subject:
        if (tag!=null) {
            sb.append(tag).append(':');
        }
        sb.append(name);
        for (int i=0; i<keyIndexes.length; i++) {
            String key=values[keyIndexes[i]];
            sb.append("_").append(key);
        }
        sb.append('\n');
        
        // construct predefined property "a"
        sb.append("    a ");
        if (dbSchema.default_tag!=null) {
            sb.append(dbSchema.default_tag).append(':');
        }
        sb.append(name).append(" ;\n");

        // construct property+subject
        for (int k=0; k<fields.size(); k++) {
            String lastChar=((k+1)<fields.size())?" ;\n":" .\n";
            CsvField field = fields.get(k);
            if (field==null) {
                throw new RuntimeException("table: '"+name+"' field "+k+" is null");
            }
            if (!field.prop) {
                continue; // not a property
            }
            sb.append("    ").append(field.getPropString())      
               .append(" ").append(field.makeSubjectString(values[k]))
               .append(lastChar);
        }
        
        String nodeString = sb.toString();
        return nodeString;
    }

    public String createUpdateString() {
        StringBuilder sb=new StringBuilder();
        sb.append("insert into ").append(getName()).append("(");
        boolean first=true;
        JsonList<CsvField> fields = getFields();
        for (CsvField field: fields) {
            if (first) {
                first=false;
            } else {
                sb.append(", ");
            }
            sb.append("\n").append(field.getName());
        }
        sb.append(")\n").append("values (");
        first=true;
        for (int k=0; k<fields.size()-1; k++) {
            sb.append("?, ");
        }
        sb.append("?)\n");
        return sb.toString();
    }

    public void setValues(PreparedStatement stmt, String[] values) throws SQLException {
        JsonList<CsvField> fields = getFields();
        for (int k=0; k<fields.size(); k++) {
            String value = values[k];
            CsvField field=fields.get(k);
            switch (field.getType()) {
            case INT:
                stmt.setLong(k+1, Long.parseLong(value));
                break;
            case REAL:
              stmt.setFloat(k+1, Float.parseFloat(value));
              break;
            case DECIMAL:
            case DOUBLE:
              stmt.setDouble(k+1, Double.parseDouble(value));
              break;
            case DATE: 
                stmt.setDate(k+1, Date.valueOf(value));
                break;
            case STR:
                stmt.setString(k+1, value);
                break;
            default:
                throw new BadSetupException("unknown field type:"+field.getType());
            }
        }
    }

    @Override
    public JsonList<?> newJsonList(String key) {
        if ("fields".equalsIgnoreCase(key)) {
            return  new Fields();
        } else if ("keys".equalsIgnoreCase(key)) {
            return  new SimpleJsonList<String>();
         } else {
             throw new ParserException("Not a List field: '"+key+'\'');
         }
     }

    public DBSchema getDbSchema() {
        return dbSchema;
    }

    public JsonList<String> getKeys() {
        return keys;
    }

    public void setKeys(JsonList<String> keys) {
        this.keys = keys;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }

    @Override
    public String getName() {
        return name;
    }

    @SuppressWarnings("unchecked")
    public JsonList<CsvField> getFields() {
        JsonList tmp=fields;
        return tmp;
    }

    public int[] getKeyIdndexes() {
        return keyIndexes;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    /** 
     *  List of mixed elements: CsvField and String, so cannot use IndexedJsonList
     * @author ak
     *
     */
    static class Fields extends SimpleJsonList<Object> {

        HashMap<String, Object> index=new HashMap<String, Object>(); 

        @Override
        public void add(Object element) {
            if (element instanceof String) {
                index.put((String)element, element);            
            } else if (element instanceof Nameable) {
                String name = ((Nameable)element).getName();
                index.put(name, element);
            }
            super.add(element);
        }
        
       public Object get(String name) {
           return index.get(name);
       }

        @Override
        public JsonObject newJsonObject(boolean ignoreCase) {
            return new CsvField();
        }
    }

}

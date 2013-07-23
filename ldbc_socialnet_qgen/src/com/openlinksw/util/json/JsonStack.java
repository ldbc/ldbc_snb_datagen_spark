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

import java.util.Stack;

/** 
 * 
 * @author ak
 *
 */
public class JsonStack extends JsonWalker {
    protected Stack<Object> stack=new Stack<Object>(); 
    protected boolean ignoreCase=false;
    protected JsonBase root=null;
    
    public JsonStack(JsonBase root, boolean ignoreCase) {
        if (root==null) {
            throw new IllegalArgumentException("root may not be null");
        }
        this.root=root;
        this.ignoreCase=ignoreCase;
    }
    
    /** put new Object on the stack
     */
    public JsonObject startObject() {
        if (stack.isEmpty()) {
            JsonObject rootObject;
            try {
                rootObject = (JsonObject) root;
            } catch (ClassCastException e) {
                throw new ParserException("Object not expected here");
            }
            stack.push(root);
            return rootObject;
        }
        JsonObject res;
        Object top = stack.peek();
        if (top instanceof String) {
            top = stack.pop();
            JsonObject object=(JsonObject) stack.peek();
            stack.push(top);
            res=object.newJsonObject((String)top, ignoreCase);
        } else if (top instanceof JsonList<?>) {
            res=((JsonList<?>)top).newJsonObject(ignoreCase);
        } else {
            throw new ParserException("Object not expected here");
        }
        stack.push(res);
        return res;
     }

    public JsonList<?> startList() {
        if (stack.isEmpty()) {
            JsonList<?> rootList;
            try {
                rootList = (JsonList<?>) root;
            } catch (ClassCastException e) {
                throw new ParserException("List not expected here");
            }
            stack.push(root);
            return rootList;
        }
        JsonList<?> res;
        Object top = stack.peek();
        if (top instanceof String) {
            top = stack.pop();
            JsonObject object=(JsonObject) stack.peek();
            stack.push(top);
            res=object.newJsonList((String)top);
        } else if (top instanceof JsonList<?>) {
            res= ((JsonList<?>)top).newJsonList();
        } else {
            throw new ParserException("List not expected here");
        }
         stack.push(res);
         return res;
      }
      
    /** general method to create an entry is the following call sequence:
     * startEntry -> (startObject|startList|pushValue) -> endEntry
     */
    public void startEntry(String key) {
        if (ignoreCase) {
            key=key.toLowerCase();
        }
        Object top = stack.peek();
        check(top instanceof JsonObject, "startEntry: not an object");
        JsonObject top2 = (JsonObject)top;
        check(top2.hasAttr(key), "no such attribute: "+key+" in class "+top2.getClass().getName());
        stack.push(key);
     }
     
    /** value is usually a string, that is, neither JsonObject nor JsonList
     * 
     * @param value
     */
    public void pushValue(String value) {
        stack.push(value);
     }
     
   public void endEntry(String key) {
        if (ignoreCase) {
            key=key.toLowerCase();
        }
        Object value = stack.pop();
        String storedKey = (String) stack.pop();
        check(storedKey.equals(key), "end entry: no stored key");
        ((JsonObject)stack.peek()).put(key, value);
     }
     
    /**
    /** general method to create a list element is the following call sequence:
     *  (startObject|startList|pushValue) -> endElement
     */
    @SuppressWarnings("unchecked")
    public void endElement() {
        Object value = stack.pop();
        Object top = stack.peek();
        if (top instanceof JsonList<?>) {
            ((JsonList<Object>)top).add(value);
        } else {
            ((JsonList<Object>)top).add(value);
        }
     }
     
     private void check(boolean condition, String message) {
         if (condition) return;
         throw new ParserException("jsonStack: check failed:"+message);
     }

    public void end() {
        stack.pop();
    }

}

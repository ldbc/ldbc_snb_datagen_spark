/**
 * 
 */
package com.openlinksw.bibm;

import com.openlinksw.bibm.tpch.ParameterPool;

public abstract class FormalParameter  {
    /** parameter which may contain spaces and other problematic characters,
     *   must be enquoted while printinq and dequoted while parsing.
     */
    private boolean quoted=false;
    private String defaultValue;
    
    public FormalParameter(boolean quoted) {
        this.quoted=quoted;
    }
    
    public FormalParameter() {
        this(false);
    }
    
	/** classes with sub-parameters should override
	 * 
	 * @param addPI
	 */
	public void init(String[] addPI) {
	    // for simple classes without parameters, do nothing 
    }

    public abstract  String getValue(ParameterPool pool);

    public void setDefaultValue(String defaultValue) {
    	if (quoted)
    	{
    		if (defaultValue.startsWith("\'") && defaultValue.endsWith("\'"))
    			this.defaultValue = defaultValue.substring(1, defaultValue.length()-1);
    		else
    			this.defaultValue = defaultValue;
    	}
    	else
    	  this.defaultValue = defaultValue;
    }

    public  String getDefaultValue() {
        return defaultValue;
    }

    public String toString(Object param) {
        if (quoted) {
            StringBuilder sb = new StringBuilder();
            sb.append('\'').append(param).append('\'');
            return sb.toString();
        } else {
            return param.toString();
        }
    }

}
package com.openlinksw.bibm.bsbm;

import java.util.List;

import com.openlinksw.bibm.Exceptions.BadSetupException;
import com.openlinksw.util.json.JsonObject;

import benchmark.generator.ValueGenerator;

public class ProdTypeLevelRange extends BSBMFormalParameter {
	List<Integer> maxProductTypePerLevel;
	int minLevel;
	int maxLevel;
	boolean lvleq;

	public ProdTypeLevelRange(BSBMParameterPool parameterPool, String[] addPI) {
		super (BSBMParameterPool.PRODUCT_TYPE_RANGE);
		int minLev ;
		int maxLev;
		if(addPI.length<2 || addPI.length>3) {
			throw new BadSetupException("Illegal number of parameters for ProductTypeRange: " + addPI.length);
		}
		try {
			minLev = Integer.parseInt(addPI[0]);
			maxLev = Integer.parseInt(addPI[1]);
			// a "lvleq"-suffix means that each level in the product type hierarchy is chosen equally
		} catch(NumberFormatException e) {
			throw new BadSetupException("Illegal parameters for ProductTypeRange: " + addPI);
		}
		
		lvleq=(addPI.length==3 && addPI[2].equals("lvleq"));
		init(parameterPool, minLev, maxLev);
	}

    public ProdTypeLevelRange(BSBMParameterPool parameterPool, JsonObject descr) {
        super (BSBMParameterPool.PRODUCT_TYPE_RANGE);
        int minLev ;
        int maxLev;

        try {
            minLev = Integer.parseInt((String) descr.get("from"));
            maxLev = Integer.parseInt((String) descr.get("to"));
            lvleq = Boolean.parseBoolean((String) descr.get("lvleq"));
            // a "lvleq"-suffix means that each level in the product type hierarchy is chosen equally
        } catch(NumberFormatException e) {
            throw new BadSetupException("Illegal parameters for ProductTypeRange: " + descr.toString());
        }
        init(parameterPool, minLev, maxLev);
    }

    private void init(BSBMParameterPool parameterPool, int minLev, int maxLev) {
        maxProductTypePerLevel=parameterPool.maxProductTypePerLevel;
		int levelNum = maxProductTypePerLevel.size();
		
		if (minLev < 0)
			minLevel = levelNum + minLev;
		else 
			minLevel = minLev;
		
		if (maxLev < 0)
			maxLevel = levelNum-1 + maxLev;
		else if (maxLev > 0)
			maxLevel = maxLev - 1;
		else
			maxLevel = levelNum-1;
		
		if (minLevel > levelNum - 1
				|| maxLevel < 0
				|| maxLevel < minLevel
				|| minLevel < 0
				|| maxLevel > levelNum - 1) {
			throw new BadSetupException("Trying to pick a random product type number from illegal level range "
							+ minLevel + " to " + maxLevel);
		}
    }
	
    public  int getRandomProductTypeNrFromRange (ValueGenerator valueGen, ValueGenerator valueGen2) {
		if (lvleq) {
			int levelToChooseFrom = valueGen2.randomInt(minLevel, maxLevel);
			return getRandomProductTypeNrFromLevel(valueGen, levelToChooseFrom, levelToChooseFrom);
		} else {			
		    return getRandomProductTypeNrFromLevel(valueGen, minLevel, maxLevel);
		}
	}

	public int getRandomProductTypeNrFromLevel(ValueGenerator valueGen, 	int minLev, int maxLev) {
		int min=minLev>0? maxProductTypePerLevel.get(minLev-1) + 1: 1;
		int max = maxProductTypePerLevel.get(maxLev);
		return valueGen.randomInt(min, max);
	}

}


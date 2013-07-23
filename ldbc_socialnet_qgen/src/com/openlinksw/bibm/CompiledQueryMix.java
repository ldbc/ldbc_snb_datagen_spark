package com.openlinksw.bibm;

import java.util.ArrayList;

public class CompiledQueryMix {
	protected Object[] queryMix;
	private int run;  // negative values are warm up runs
	
	public CompiledQueryMix(Object[] compiledQueries, int run) {
		this.queryMix = compiledQueries;
		this.run = run;
	}

    public CompiledQueryMix(ArrayList<Object> queryMixRun, int run) {
        this(queryMixRun.toArray(new Object[queryMixRun.size()]), run);
    }

	public Object[] getQueryMix() {
        return queryMix;
    }

    public int getRun() {
        return run;
    }

}

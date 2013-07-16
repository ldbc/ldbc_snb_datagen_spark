package com.openlinksw.bibm.connection;

import com.openlinksw.bibm.AbstractQueryResult;
import com.openlinksw.bibm.CompiledQuery;

public interface ServerConnection {
	/*
	 * Execute Query with Query Object
	 */
	
	public AbstractQueryResult executeQuery(CompiledQuery query) throws InterruptedException;

	
	public void close();
}


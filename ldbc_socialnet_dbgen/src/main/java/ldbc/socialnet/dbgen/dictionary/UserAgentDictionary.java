/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
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
package ldbc.socialnet.dbgen.dictionary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;
import java.util.Random;


public class UserAgentDictionary {
    
	String fileName;
	
	Vector<String> userAgents;
	double probSentFromAgent; 
	
	public UserAgentDictionary(String fileName, double probSentFromAgent){
		this.fileName = fileName; 
		this.probSentFromAgent = probSentFromAgent;
	}
	
	public void init(){
		try {
		    userAgents = new Vector<String>();
			BufferedReader agentFile = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));
			String line; 
			while ((line = agentFile.readLine()) != null) {
			    userAgents.add(line.trim());
            }
			agentFile.close();
			System.out.println("Done ... " + userAgents.size() + " agents have been extracted ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public String getUserAgentName(Random random, boolean hasSmathPhone, byte agentId){
		return (hasSmathPhone && (random.nextDouble() > probSentFromAgent)) ? userAgents.get(agentId) : "";
	}
	
	public byte getRandomUserAgentIdx(Random random){
		return (byte)random.nextInt(userAgents.size());
	}	
}

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
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.IP;


public class IPAddressDictionary {
    
    private static final String SEPARATOR_COUNTRY = "   ";
    private static final String SEPARATOR_IP = "[.]";
    private static final String SEPARATOR_MASK = "/";
    private static final int MAX_IP_COUNTRY = 100;
    
    
    
	HashMap<Integer, Vector<IP>> ipsByCountry;
	HashMap<Integer, Integer> ipCountry;
	
	LocationDictionary locationDic;
	
	String 	mappingFileName;
	String 	baseIPdir;
	
	Random randIP;
	Random randDiffIP; 
	Random randDiffIPforTravellers;
	double probDiffIPinTravelSeason;
    double probDiffIPnotTravelSeason;
    double probDiffIPforTraveller;
	
	public IPAddressDictionary(String _mappingFileName, String _baseIPdir, LocationDictionary locationDic, 
								long seedIP, double _probDiffIPinTravelSeason, 
								double _probDiffIPnotTravelSeason, double _probDiffIPforTraveller){
		this.mappingFileName = _mappingFileName;
		this.baseIPdir = _baseIPdir;
		
		this.locationDic = locationDic;
		ipCountry = new HashMap<Integer, Integer>();
		ipsByCountry = new HashMap<Integer, Vector<IP>>();
		
		probDiffIPinTravelSeason = _probDiffIPinTravelSeason; 
		probDiffIPnotTravelSeason = _probDiffIPnotTravelSeason;
		probDiffIPforTraveller = _probDiffIPforTraveller;
		
		randIP = new Random(seedIP);
		randDiffIP = new Random(seedIP);
		randDiffIPforTravellers = new Random(seedIP);
	}
	
	public void initialize() {
	    String line;
	    HashMap<String, String> countryAbbreMap = new HashMap<String, String>();
	    try {
	        BufferedReader mappingFile = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(mappingFileName), "UTF-8"));
	        while ((line = mappingFile.readLine()) != null){
	            String data[] = line.split(SEPARATOR_COUNTRY);
	            String abbr = data[0];
	            String countryName = data[1].trim().replace(" ", "_");
	            countryAbbreMap.put(countryName, abbr);
	        }
	        mappingFile.close();

	        Vector<Integer> countries = locationDic.getCountries();
	        for (int i = 0; i < countries.size(); i ++) {
	            ipsByCountry.put(countries.get(i), new Vector<IP>());

	            //Get the name of file
	            String fileName = countryAbbreMap.get(locationDic.getLocationName(countries.get(i)));
	            fileName = baseIPdir + "/" + fileName + ".zone";
	            BufferedReader ipZoneFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));

	            int j = 0;
	            while ((line = ipZoneFile.readLine()) != null && (j < MAX_IP_COUNTRY)) {
	                String data[] = line.split(SEPARATOR_IP);
	                String maskData[] = data[3].split(SEPARATOR_MASK);
	                int byte1 = Integer.valueOf(data[0]);
	                int byte2 = Integer.valueOf(data[1]);
	                int byte3 = Integer.valueOf(data[2]);
	                int byte4 = Integer.valueOf(maskData[0]);
	                int maskNum = Integer.valueOf(maskData[1]);

	                IP ip = new IP(byte1, byte2, byte3, byte4, maskNum);

	                ipsByCountry.get(i).add(ip);
	                ipCountry.put(ip.getIp() & ~ip.getMask(), i);
	                j++;
	            }
	            ipZoneFile.close();
	        } 
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

	public int getLocation(IP ip) {
	    int network = ip.getIp() & ~ip.getMask();
	    return (ipCountry.containsKey(network)) ? ipCountry.get(network) : -1;
	}
	
	public IP getRandomIPFromLocation(Random random, int locationIdx) {
		Vector<IP> countryIPs = ipsByCountry.get(locationIdx);
		int idx = random.nextInt(countryIPs.size());
		
		IP networkIp = countryIPs.get(idx);
		
		int formattedIP = 0;
		int ip = networkIp.getIp();
		int mask = networkIp.getMask();
		
		for (int i = 0; i < IP.IP4_SIZE_BYTES; i++) {
		    int randomRange = ((mask >>> (IP.BYTE_SIZE * i)) & 0xFF) + 1;
		    int base = ((ip >>> (IP.BYTE_SIZE * i)) & 0xFF) + random.nextInt(randomRange);
		    formattedIP = formattedIP | (base << IP.BYTE_SIZE * i);
        }
		
		return new IP(formattedIP, mask);
	}
	
	public IP getRandomIP(Random random) {
	    Vector<Integer> countries = locationDic.getCountries();
        int randomLocationIdx = random.nextInt(countries.size());
		return getRandomIPFromLocation(randomLocationIdx);
	}
	
	private boolean changeUsualIp(Random random, boolean isFrequentChange, long date) {
        double randomNumber = random.nextDouble();
        boolean isTravelSeason = DateGenerator.isTravelSeason(date);
        if ( (isFrequentChange && randomNumber < probDiffIPforTraveller ) || 
             (!isFrequentChange && ((isTravelSeason && randomNumber < probDiffIPinTravelSeason) ||
                                   (!isTravelSeason && randomNumber < probDiffIPnotTravelSeason)))) {
                 return true;
        }
        return false;
	}
	
	public IP getIP(Random random, IP ip, boolean isFrequentChange, long date) {
	    return (changeUsualIp(random, isFrequentChange, date)) ? new IP(ip.getIp(), ip.getMask()) : getRandomIP(random);
	}
	
	public IP getIP(Random random, IP ip, boolean isFrequentChange, long date, int countryId) {
        return (changeUsualIp(random, isFrequentChange, date)) ? new IP(ip.getIp(), ip.getMask()) : getRandomIPFromLocation(random, countryId);
    }
}

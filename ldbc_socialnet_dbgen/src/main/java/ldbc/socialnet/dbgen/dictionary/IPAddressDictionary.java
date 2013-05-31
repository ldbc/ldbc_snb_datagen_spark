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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;

import ldbc.socialnet.dbgen.generator.DateGenerator;
import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.IP;
import ldbc.socialnet.dbgen.objects.Location;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.Post;


public class IPAddressDictionary {
	Vector<Vector<IPRange>> vIPDic; 		// Store the IP ranges by the countries
	
	HashMap<String, String> countryAbbreMap;
	
	Vector<Location> vLocation;
	
	String 	mappingFileName;
	String 	baseIPdir; 
	int 	maxNumIPRanges = 100; 
	
	Random	randIP;
	double 	probDiffIPinTravelSeason;
	double 	probDiffIPnotTravelSeason;
	double 	probDiffIPforTraveller;
	Random 	randDiffIP; 
	Random	randDiffIPforTravellers;
	
	public IPAddressDictionary(String _mappingFileName, String _baseIPdir, Vector<Location> _vLocation, 
								long seedIP, double _probDiffIPinTravelSeason, 
								double _probDiffIPnotTravelSeason, double _probDiffIPforTraveller){
		this.mappingFileName = _mappingFileName;
		this.baseIPdir = _baseIPdir;
		
		countryAbbreMap = new HashMap<String, String>();
		
		vLocation = _vLocation;
		vIPDic = new Vector<Vector<IPRange>>();
		
		probDiffIPinTravelSeason = _probDiffIPinTravelSeason; 
		probDiffIPnotTravelSeason = _probDiffIPnotTravelSeason;
		probDiffIPforTraveller = _probDiffIPforTraveller;
		
		randIP = new Random(seedIP);
		randDiffIP = new Random(seedIP);
		randDiffIPforTravellers = new Random(seedIP);
	}
	
	public void init(){
		readMappingCoutryName();
		//checkCountryNameExistence();
		extractIPAddress();
	}
	
	public void readMappingCoutryName(){
		String line; 
		String abbr;
		String countryName; 
		try {
		    BufferedReader mappingFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(mappingFileName), "UTF-8"));
			while ((line = mappingFile.readLine()) != null){
				String infos[] = line.split("   ");
				abbr = infos[0];
				countryName = infos[1].trim().replace(" ", "_");
				//System.out.println(infos[0]);
				//System.out.println(infos[1]);
				countryAbbreMap.put(countryName, abbr);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void extractIPAddress(){
		String line; 
		
		for (int i = 0; i < vLocation.size(); i ++){
			vIPDic.add(new Vector<IPRange>());
			
			//Get the name of file
			String fileName = countryAbbreMap.get(vLocation.get(i).getName());
			fileName = baseIPdir + "/" + fileName + ".zone";
			
			int j = 0; 
			try {
			    BufferedReader ipZoneFile = new BufferedReader(new InputStreamReader(getClass( ).getResourceAsStream(fileName), "UTF-8"));
				
				//System.out.println(fileName);
				
				while ((line = ipZoneFile.readLine()) != null){
					IPRange iprange = new IPRange(); 
					line = line.replace(".", " ");
					String infos[] = line.split(" ");
					//System.out.println(line);
					iprange.setIp1(Short.parseShort(infos[0]));
					iprange.setIp2(Short.parseShort(infos[1]));
					iprange.setIp3(Short.parseShort(infos[2]));
					
					String ranges[] = infos[3].split("/");
					
					short ip4first = Short.parseShort(ranges[0]);
					short ip4second = Short.parseShort(ranges[1]);
					if (ip4first < ip4second){
						iprange.setIp4start(ip4first);
						iprange.setIp4end(ip4second);
					}
					else{ 
						iprange.setIp4end(ip4first);
						iprange.setIp4start(ip4second);
					}
					
					
					
					vIPDic.get(i).add(iprange);
					
					j++;
					if (j == maxNumIPRanges) break;
				}
				
				ipZoneFile.close();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void checkCountryNameExistence(){
		for (int i = 0; i < vLocation.size(); i ++){
			String countryName = vLocation.get(i).getName();
			if (!countryAbbreMap.containsKey(countryName)){
				System.out.println("Country " + countryName + " is not in the mapping file");
			}
		}
	}
	
	public int getLocation(IP ip) {
	    for (int i = 0; i < vIPDic.size(); i ++) {
	        for (int j = 0; j < vIPDic.get(i).size(); j++) {
	            IPRange target = vIPDic.get(i).get(j);
	            if (ip.getIp1() == target.getIp1() && ip.getIp2() == target.getIp2() &&
	                    ip.getIp3() == target.getIp3() && ip.getIp4() >= target.getIp4start() &&
	                    ip.getIp4() <= target.getIp4end()) {
	                return i;
	            }
	        }
	    }
	    return -1;
	}
	
	public IP getRandomIPAddressFromLocation(int locationIdx){
		Vector<IPRange> countryIPs = vIPDic.get(locationIdx);
		int idx = randIP.nextInt(countryIPs.size());
		
		IPRange iprange = countryIPs.get(idx);
		short ip4 =-1;
		if (iprange.ip4end == iprange.ip4start) ip4 = iprange.ip4start;
		else{
			try {
				ip4 = (short)(randIP.nextInt(iprange.ip4end - iprange.ip4start) + iprange.ip4start);
			} catch (Exception e) {
				System.out.println(" iprange.ip4end = " + iprange.ip4end);
				System.out.println(" iprange.ip4start = " + iprange.ip4start);
				e.printStackTrace();
				System.exit(-1);
			}
		}
		
		IP ip = new IP(iprange.ip1,iprange.ip2,iprange.ip3,ip4);
		return ip;
	}
	public IP getRandomIP(){
		int randomLocationIdx = randIP.nextInt(vLocation.size());
		return getRandomIPAddressFromLocation(randomLocationIdx);
	}
	
	// Only 1% users (i.e., users with frequent-change property) can have variation of IP address
	// Other users have the IP addresses correlated with the location
	// For these 1% users, probability that they have different IP address is 0.5
	
	public void setPostIPAdress(boolean isFrequentChange, IP ipAdress, Post post){
		
		// Check whether the posting time is on a vacation season
		if (isFrequentChange )
		{
			if (randDiffIPforTravellers.nextDouble() < probDiffIPforTraveller){
				post.setIpAddress(getRandomIP());
			}
		}
		else{
			// check whether it is a travel season
			if (DateGenerator.isTravelSeason(post.getCreatedDate())){
				if (randDiffIP.nextDouble() < probDiffIPinTravelSeason){
					post.setIpAddress(getRandomIP());
					return; 
				}
			}
			else{
				if (randDiffIP.nextDouble() < probDiffIPnotTravelSeason){
					post.setIpAddress(getRandomIP());
					return;
				}
			}
		}
		
		post.setIpAddress(ipAdress);
	}
	
	public void setCommentIPAdress(boolean isFrequentChange, IP ipAdress, Comment comment){
		
		// Check whether the posting time is on a vacation season
		if (isFrequentChange )
		{
			if (randDiffIPforTravellers.nextDouble() < probDiffIPforTraveller){
				comment.setIpAddress(getRandomIP());
			}
		}
		else{
			// check whether it is a travel season
			if (DateGenerator.isTravelSeason(comment.getCreateDate())){
				if (randDiffIP.nextDouble() < probDiffIPinTravelSeason){
					comment.setIpAddress(getRandomIP());
					return; 
				}
			}
			else{
				if (randDiffIP.nextDouble() < probDiffIPnotTravelSeason){
					comment.setIpAddress(getRandomIP());
					return;
				}
			}
		}
		
		comment.setIpAddress(ipAdress);
	}
	
	public void setPhotoIPAdress(boolean isFrequentChange, IP ipAdress, Photo photo){
		
		// Check whether the posting time is on a vacation season
		if (isFrequentChange )
		{
			if (randDiffIPforTravellers.nextDouble() < probDiffIPforTraveller){
				photo.setIpAddress(getRandomIPAddressFromLocation(photo.getLocationIdx()));
			}
		}
		else{
			// check whether it is a travel season
			if (DateGenerator.isTravelSeason(photo.getTakenTime())){
				if (randDiffIP.nextDouble() < probDiffIPinTravelSeason){
					photo.setIpAddress(getRandomIPAddressFromLocation(photo.getLocationIdx()));
					return; 
				}
			}
			else{
				if (randDiffIP.nextDouble() < probDiffIPnotTravelSeason){
					photo.setIpAddress(getRandomIPAddressFromLocation(photo.getLocationIdx()));
					return;
				}
			}
		}
		
		photo.setIpAddress(ipAdress);
	}
	
}

class IPRange{
	short ip1; 
	short ip2;
	short ip3;
	short ip4start;
	short ip4end;
	
	public short getIp1() {
		return ip1;
	}
	public void setIp1(short ip1) {
		this.ip1 = ip1;
	}
	public short getIp2() {
		return ip2;
	}
	public void setIp2(short ip2) {
		this.ip2 = ip2;
	}
	public short getIp3() {
		return ip3;
	}
	public void setIp3(short ip3) {
		this.ip3 = ip3;
	}
	public short getIp4start() {
		return ip4start;
	}
	public void setIp4start(short ip4start) {
		this.ip4start = ip4start;
	}
	public short getIp4end() {
		return ip4end;
	}
	public void setIp4end(short ip4end) {
		this.ip4end = ip4end;
	}
}

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
package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.DateGenerator;
import ldbc.snb.datagen.objects.IP;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;


public class IPAddressDictionary {

    private static final String SEPARATOR_COUNTRY = "   ";
    private static final String SEPARATOR_IP = "[.]";
    private static final String SEPARATOR_MASK = "/";
    private static final int MAX_IP_COUNTRY = 100;
    private HashMap<Integer, ArrayList<IP>> ipsByCountry;
    /**
     * < @brief The ips by country. *
     */
    private HashMap<Integer, Integer> ipCountry;
    /**
     * < @brief The country of ips. *
     */
    private PlaceDictionary placeDictionary;
    /**
     * < @brief The location dictionary. *
     */
    private double probDiffIPinTravelSeason;
    private double probDiffIPnotTravelSeason;

    public IPAddressDictionary(PlaceDictionary locationDic,
                               double _probDiffIPinTravelSeason,
                               double _probDiffIPnotTravelSeason) {

        this.placeDictionary = locationDic;
        this.ipCountry = new HashMap<Integer, Integer>();
        this.ipsByCountry = new HashMap<Integer, ArrayList<IP>>();
        this.probDiffIPinTravelSeason = _probDiffIPinTravelSeason;
        this.probDiffIPnotTravelSeason = _probDiffIPnotTravelSeason;
	load(DatagenParams.countryAbbrMappingFile,DatagenParams.IPZONE_DIRECTORY);
    }

    /**
     * @param mappingFileName The abbreviations per country.
     * @param baseIPdir       The base directory where ip files are found.
     * @breif Loads dictionary.
     */
    private void load(String mappingFileName, String baseIPdir) {
        String line;
        HashMap<String, String> countryAbbreMap = new HashMap<String, String>();
        try {
            BufferedReader mappingFile = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(mappingFileName), "UTF-8"));
            while ((line = mappingFile.readLine()) != null) {
                String data[] = line.split(SEPARATOR_COUNTRY);
                String abbr = data[0];
                String countryName = data[1].trim().replace(" ", "_");
                countryAbbreMap.put(countryName, abbr);
            }
            mappingFile.close();

            ArrayList<Integer> countries = placeDictionary.getCountries();
            for (int i = 0; i < countries.size(); i++) {
                ipsByCountry.put(countries.get(i), new ArrayList<IP>());

                //Get the name of file
                String fileName = countryAbbreMap.get(placeDictionary.getPlaceName(countries.get(i)));
                fileName = baseIPdir + "/" + fileName + ".zone";
                BufferedReader ipZoneFile = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(fileName), "UTF-8"));

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
        while (placeDictionary.getType(locationIdx) != "country") {
            locationIdx = placeDictionary.belongsTo(locationIdx);
        }
        ArrayList<IP> countryIPs = ipsByCountry.get(locationIdx);
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
        ArrayList<Integer> countries = placeDictionary.getCountries();
        int randomLocationIdx = random.nextInt(countries.size());
        return getRandomIPFromLocation(random, randomLocationIdx);
    }

    private boolean changeUsualIp(Random randomDiffIP, Random randomDiffIPForTravelers, long date) {
        double diffIpProb = randomDiffIP.nextDouble();
        double diffIpForTravelersProb = randomDiffIPForTravelers.nextDouble();
        boolean isTravelSeason = DateGenerator.isTravelSeason(date);
                if ((isTravelSeason && diffIpForTravelersProb < probDiffIPinTravelSeason) ||
                        (!isTravelSeason && diffIpForTravelersProb < probDiffIPnotTravelSeason)) {
            return true;
        }
        return false;
    }

    public IP getIP(Random randomIP, Random randomDiffIP, Random randomDiffIPForTravelers, IP ip, long date) {
        return (changeUsualIp(randomDiffIP, randomDiffIPForTravelers, date)) ? new IP(ip.getIp(), ip.getMask()) : getRandomIP(randomIP);
    }

    public IP getIP(Random randomIP, Random randomDiffIP, Random randomDiffIPForTravelers, IP ip, long date, int countryId) {
        return (changeUsualIp(randomDiffIP, randomDiffIPForTravelers, date)) ? new IP(ip.getIp(), ip.getMask()) : getRandomIPFromLocation(randomIP, countryId);
    }
}

/* 
 Copyright (c) 2013 LDBC
 Linked Data Benchmark Council (http://www.ldbcouncil.org)
 
 This file is part of ldbc_snb_datagen.
 
 ldbc_snb_datagen is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 ldbc_snb_datagen is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with ldbc_snb_datagen.  If not, see <http://www.gnu.org/licenses/>.
 
 Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 All Rights Reserved.
 
 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation;  only Version 2 of the License dated
 June 1991.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.*/
package ldbc.snb.datagen.dictionary;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.DateGenerator;
import org.apache.hadoop.conf.Configuration;

import java.util.GregorianCalendar;

/**
 * @author aprat
 */
public class Dictionaries {

    public static BrowserDictionary browsers = null;
    public static CompanyDictionary companies = null;
    public static DateGenerator dates = null;
    public static EmailDictionary emails = null;
    public static IPAddressDictionary ips = null;
    public static LanguageDictionary languages = null;
    public static NamesDictionary names = null;
    public static PlaceDictionary places = null;
    public static PopularPlacesDictionary popularPlaces = null;
    public static TagDictionary tags = null;
    public static TagMatrix tagMatrix = null;
    public static TagTextDictionary tagText = null;
    public static UniversityDictionary universities = null;
    public static FlashmobTagDictionary flashmobs = null;


    public static void loadDictionaries(Configuration conf) {

        browsers = new BrowserDictionary(DatagenParams.probAnotherBrowser);

        dates = new DateGenerator(conf, new GregorianCalendar(DatagenParams.startYear,
                                                              DatagenParams.startMonth,
                                                              DatagenParams.startDate),
                                  new GregorianCalendar(DatagenParams.endYear,
                                                        DatagenParams.endMonth,
                                                        DatagenParams.endDate),
                                  DatagenParams.alpha
        );


        emails = new EmailDictionary();

        places = new PlaceDictionary();

        ips = new IPAddressDictionary(places);


        languages = new LanguageDictionary(places,
                                           DatagenParams.probEnglish,
                                           DatagenParams.probSecondLang);

        names = new NamesDictionary(places);

        popularPlaces = new PopularPlacesDictionary(places);

        tags = new TagDictionary(places.getCountries().size(),
                                 DatagenParams.tagCountryCorrProb);

        tagMatrix = new TagMatrix();

        companies = new CompanyDictionary(places, DatagenParams.probUnCorrelatedCompany);

        universities = new UniversityDictionary(places,
                                                DatagenParams.probUnCorrelatedOrganization,
                                                DatagenParams.probTopUniv,
                                                companies.getNumCompanies());

        flashmobs = new FlashmobTagDictionary(tags,
                                              dates,
                                              DatagenParams.flashmobTagsPerMonth,
                                              DatagenParams.probInterestFlashmobTag,
                                              DatagenParams.probRandomPerLevel,
                                              DatagenParams.flashmobTagMinLevel,
                                              DatagenParams.flashmobTagMaxLevel,
                                              DatagenParams.flashmobTagDistExp);

        tagText = new TagTextDictionary(tags, DatagenParams.ratioReduceText);
    }

}

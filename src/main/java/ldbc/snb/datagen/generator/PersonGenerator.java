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
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;
import org.apache.hadoop.conf.Configuration;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.GregorianCalendar;

/**
 * Created by aprat on 10/7/14.
 */
public class PersonGenerator {

    private DegreeDistribution degreeDistribution_ = null;
    private PowerDistGenerator randomTagPowerLaw = null;
    private RandomGeneratorFarm randomFarm = null;
    private int nextId = 0;

    public PersonGenerator(Configuration conf, String degreeDistribution) {
        try {
            degreeDistribution_ = (DegreeDistribution) Class.forName(degreeDistribution).newInstance();
            degreeDistribution_.initialize(conf);
        } catch (ClassNotFoundException e) {
            System.out.print(e.getMessage());
        } catch (IllegalAccessException e) {
            System.out.print(e.getMessage());
        } catch (InstantiationException e) {
            System.out.print(e.getMessage());
        }

        randomTagPowerLaw = new PowerDistGenerator(DatagenParams.minNumTagsPerUser,
                                                   DatagenParams.maxNumTagsPerUser + 1,
                                                   DatagenParams.alpha);
        randomFarm = new RandomGeneratorFarm();
    }

    private long composeUserId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 41);
        long bucket = (long) (256 * (date - Dictionaries.dates.getStartDateTime()) / (double) Dictionaries.dates
                .getEndDateTime());
        return (bucket << 41) | ((id & idMask));
    }


    private Person generateUser() {

        long creationDate = Dictionaries.dates.randomPersonCreationDate(randomFarm
                                                                                .get(RandomGeneratorFarm.Aspect.DATE));
        int countryId = Dictionaries.places.getCountryForUser(randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY));
        Person person = new Person();
        person.creationDate(creationDate);
        person.gender((randomFarm.get(RandomGeneratorFarm.Aspect.GENDER).nextDouble() > 0.5) ? (byte) 1 : (byte) 0);
        person.birthDay(Dictionaries.dates
                                .getBirthDay(randomFarm.get(RandomGeneratorFarm.Aspect.BIRTH_DAY), creationDate));
        person.browserId(Dictionaries.browsers.getRandomBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER)));
        person.countryId(countryId);
        person.cityId(Dictionaries.places.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.CITY), countryId));
        person.ipAddress(Dictionaries.ips.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), countryId));
        person.maxNumKnows(Math.min(degreeDistribution_.nextDegree(), DatagenParams.numPersons));
        person.accountId(composeUserId(nextId++, creationDate));
        person.mainInterest(Dictionaries.tags.getaTagByCountry(randomFarm
                                                                       .get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), randomFarm
                                                                       .get(RandomGeneratorFarm.Aspect.TAG), person
                                                                       .countryId()));
        short numTags = ((short) randomTagPowerLaw.getValue(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_TAG)));
        person.interests(Dictionaries.tagMatrix
                                 .getSetofTags(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm
                                         .get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), person
                                                       .mainInterest(), numTags));
        person.universityLocationId(Dictionaries.universities.getRandomUniversity(randomFarm, person.countryId()));
        person.randomId(randomFarm.get(RandomGeneratorFarm.Aspect.RANDOM).nextInt(Integer.MAX_VALUE) % 100);

        person.firstName(Dictionaries.names.getRandomGivenName(randomFarm.get(RandomGeneratorFarm.Aspect.NAME),
                                                               person.countryId(),
                                                               person.gender() == 1,
                                                               Dictionaries.dates.getBirthYear(person.birthDay())));

        person.lastName(Dictionaries.names.getRandomSurname(randomFarm.get(RandomGeneratorFarm.Aspect.SURNAME), person
                .countryId()));

        int numEmails = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextInt(DatagenParams.maxEmails) + 1;
        double prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        /*if (prob >= DatagenParams.missingRatio)*/
        {
            String base = person.firstName();
            base = Normalizer.normalize(base, Normalizer.Form.NFD);
            base = base.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
            base = base.replaceAll(" ", ".");
            base = base.replaceAll("[.]+", ".");
            for (int i = 0; i < numEmails; i++) {
                String email = base + "" + person.accountId() + "@" +
                        Dictionaries.emails.getRandomEmail(randomFarm.get(RandomGeneratorFarm.Aspect.TOP_EMAIL),
                                                           randomFarm.get(RandomGeneratorFarm.Aspect.EMAIL));
                person.emails().add(email);
            }
        }

        // Set class year
        prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if ((prob < DatagenParams.missingRatio) || person.universityLocationId() == -1) {
            person.classYear(-1);
        } else {
            person.classYear(Dictionaries.dates.getClassYear(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                                                             person.creationDate(),
                                                             person.birthDay()));
        }

        // Set company and workFrom
        int numCompanies = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO)
                                     .nextInt(DatagenParams.maxCompanies) + 1;
        prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if (prob >= DatagenParams.missingRatio) {
            for (int i = 0; i < numCompanies; i++) {
                long workFrom;
                workFrom = Dictionaries.dates.getWorkFromYear(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                                                              person.classYear(),
                                                              person.birthDay());
                long company = Dictionaries.companies.getRandomCompany(randomFarm, person.countryId());
                person.companies().put(company, workFrom);
            }
        }

        ArrayList<Integer> userLanguages = Dictionaries.languages.getLanguages(randomFarm
                                                                                       .get(RandomGeneratorFarm.Aspect.LANGUAGE),
                                                                               person.countryId());
        int internationalLang = Dictionaries.languages.getInternationlLanguage(randomFarm
                                                                                       .get(RandomGeneratorFarm.Aspect.LANGUAGE));
        if (internationalLang != -1 && userLanguages.indexOf(internationalLang) == -1) {
            userLanguages.add(internationalLang);
        }
        person.languages().addAll(userLanguages);


        // Set activity characteristics
        person.isLargePoster(isLargePoster(person));
        return person;
    }

    private boolean isLargePoster(Person p) {
        return Dictionaries.dates.getBirthMonth(p.birthDay()) == GregorianCalendar.JANUARY;
    }

    private void resetState(int blockId) {
        degreeDistribution_.reset(blockId);
        randomFarm.resetRandomGenerators((long) blockId);
    }

    /**
     * Generates a block of persons
     *
     * @param seed      The seed to feed the pseudo-random number generators.
     * @param blockSize The size of the block of persons to generate.
     * @return
     */
    public Person[] generateUserBlock(int seed, int blockSize) {
        resetState(seed);
        nextId = seed * blockSize;
        SN.machineId = seed;
        Person[] block;
        block = new Person[blockSize];
        for (int j = 0; j < blockSize; ++j) {
            block[j] = generateUser();
//            System.out.println(j);
        }
        return block;
    }
}

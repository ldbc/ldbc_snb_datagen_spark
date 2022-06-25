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
package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.generator.DatagenParams;
import ldbc.snb.datagen.generator.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.generator.distribution.DegreeDistribution;
import ldbc.snb.datagen.generator.tools.PowerDistribution;
import ldbc.snb.datagen.util.DateUtils;
import ldbc.snb.datagen.util.GeneratorConfiguration;
import ldbc.snb.datagen.util.PersonDeleteDistribution;
import ldbc.snb.datagen.util.RandomGeneratorFarm;

import java.text.Normalizer;
import java.time.Month;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PersonGenerator {

    private PersonDeleteDistribution personDeleteDistribution;
    private DegreeDistribution degreeDistribution;
    private PowerDistribution randomTagPowerLaw;
    private RandomGeneratorFarm randomFarm;
    private int nextId = 0;

    public PersonGenerator(GeneratorConfiguration conf, String degreeDistribution) {
//        try {
//            this.degreeDistribution = (DegreeDistribution) Class.forName(degreeDistribution).newInstance();
            this.degreeDistribution = DatagenParams.getDegreeDistribution();
            this.personDeleteDistribution = new PersonDeleteDistribution(DatagenParams.personDeleteFile);
            personDeleteDistribution.initialize();
            this.degreeDistribution.initialize(conf);

//        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
//            System.out.print(e.getMessage());
//        }

        randomTagPowerLaw = new PowerDistribution(DatagenParams.minNumTagsPerPerson,
                DatagenParams.maxNumTagsPerPerson + 1,
                DatagenParams.alpha);
        randomFarm = new RandomGeneratorFarm();
    }

    private long composePersonId(long id, long date) {
        long idMask = ~(0xFFFFFFFFFFFFFFFFL << 41);
        long bucket = (long) (256 * (date - Dictionaries.dates.getSimulationStart()) / (double) Dictionaries.dates
                .getSimulationEnd());
        return (bucket << 41) | ((id & idMask));
    }


    private Person generatePerson() {

        long creationDate = Dictionaries.dates.randomPersonCreationDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE));

        int countryId = Dictionaries.places.getCountryForPerson(randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY));
        Person person = new Person();
        person.setCreationDate(creationDate);

        person.setGender((randomFarm.get(RandomGeneratorFarm.Aspect.GENDER).nextDouble() > 0.5) ? (byte) 1 : (byte) 0);
        person.setBirthday(Dictionaries.dates
                .getBirthDay(randomFarm.get(RandomGeneratorFarm.Aspect.BIRTH_DAY)));
        person.setBrowserId(Dictionaries.browsers.getRandomBrowserId(randomFarm.get(RandomGeneratorFarm.Aspect.BROWSER)));
        person.setCountryId(countryId);
        person.setCityId(Dictionaries.places.getRandomCity(randomFarm.get(RandomGeneratorFarm.Aspect.CITY), countryId));
        person.setIpAddress(Dictionaries.ips.getIP(randomFarm.get(RandomGeneratorFarm.Aspect.IP), countryId));

        person.setMessageDeleter(randomFarm.get(RandomGeneratorFarm.Aspect.RANDOM).nextDouble() > 0.5);

        long maxKnows = Math.min(degreeDistribution.nextDegree(), DatagenParams.maxNumFriends);
        person.setMaxNumKnows(maxKnows);


        long deletionDate;
        boolean delete = personDeleteDistribution.isDeleted(randomFarm.get(RandomGeneratorFarm.Aspect.DELETION_PERSON), maxKnows);
        if (delete) {
            person.setExplicitlyDeleted(true);
            long maxDeletionDate = Dictionaries.dates.getSimulationEnd();
            deletionDate = Dictionaries.dates.randomPersonDeletionDate(
                    randomFarm.get(RandomGeneratorFarm.Aspect.DATE), creationDate, person.getMaxNumKnows(), maxDeletionDate);
        } else {
            person.setExplicitlyDeleted(false);
            deletionDate = Dictionaries.dates.getNetworkCollapse();
        }
        person.setDeletionDate(deletionDate);


        assert (person.getCreationDate() + DatagenParams.delta <= person.getDeletionDate()) : "Person creation date is larger than person deletion date";

        person.setAccountId(composePersonId(nextId++, creationDate));
        person.setMainInterest(Dictionaries.tags.getaTagByCountry(randomFarm
                .get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), randomFarm
                .get(RandomGeneratorFarm.Aspect.TAG), person
                .getCountryId()));
        short numTags = ((short) randomTagPowerLaw.getValue(randomFarm.get(RandomGeneratorFarm.Aspect.NUM_TAG)));
        person.setInterests(new ArrayList<>(Dictionaries.tagMatrix
                .getSetofTags(randomFarm.get(RandomGeneratorFarm.Aspect.TOPIC), randomFarm
                        .get(RandomGeneratorFarm.Aspect.TAG_OTHER_COUNTRY), person
                        .getMainInterest(), numTags)));
        person.setUniversityLocationId(Dictionaries.universities.getRandomUniversity(randomFarm, person.getCountryId()));
        person.setRandomId(randomFarm.get(RandomGeneratorFarm.Aspect.RANDOM).nextInt(Integer.MAX_VALUE) % 100);

        person.setFirstName(Dictionaries.names.getRandomGivenName(randomFarm.get(RandomGeneratorFarm.Aspect.NAME),
                person.getCountryId(),
                person.getGender() == 1,
                DateUtils.getYear(person.getBirthday())));

        person.setLastName(Dictionaries.names.getRandomSurname(randomFarm.get(RandomGeneratorFarm.Aspect.SURNAME), person
                .getCountryId()));

        int numEmails = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextInt(DatagenParams.maxEmails) + 1;
        double prob;
        String base = person.getFirstName();
        base = Normalizer.normalize(base, Normalizer.Form.NFD);
        base = base.replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        base = base.replaceAll(" ", ".");
        base = base.replaceAll("[.]+", ".");
        while (person.getEmails().size() < numEmails) {
            String email = base + "" + person.getAccountId() + "@" +
                    Dictionaries.emails.getRandomEmail(randomFarm.get(RandomGeneratorFarm.Aspect.TOP_EMAIL),
                            randomFarm.get(RandomGeneratorFarm.Aspect.EMAIL));
            // avoid duplicates
            if (!person.getEmails().contains(email)) {
                person.getEmails().add(email);
            }
        }

        // Set class year
        prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if ((prob < DatagenParams.missingRatio) || person.getUniversityLocationId() == -1) {
            person.setClassYear(-1);
        } else {
            person.setClassYear(Dictionaries.dates.randomClassYear(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                    person.getBirthday()));
        }

        // Set company and workFrom
        int numCompanies = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO)
                .nextInt(DatagenParams.maxCompanies) + 1;
        prob = randomFarm.get(RandomGeneratorFarm.Aspect.EXTRA_INFO).nextDouble();
        if (prob >= DatagenParams.missingRatio) {
            for (int i = 0; i < numCompanies; i++) {
                long workFrom;
                workFrom = Dictionaries.dates.getWorkFromYear(randomFarm.get(RandomGeneratorFarm.Aspect.DATE),
                        person.getClassYear(),
                        person.getBirthday());
                long company = Dictionaries.companies.getRandomCompany(randomFarm, person.getCountryId());
                person.getCompanies().put(company, workFrom);
            }
        }

        List<Integer> personLanguages = Dictionaries.languages.getLanguages(randomFarm
                        .get(RandomGeneratorFarm.Aspect.LANGUAGE),
                person.getCountryId());
        int internationalLang = Dictionaries.languages.getInternationlLanguage(randomFarm
                .get(RandomGeneratorFarm.Aspect.LANGUAGE));
        if (internationalLang != -1 && personLanguages.indexOf(internationalLang) == -1) {
            personLanguages.add(internationalLang);
        }
        person.getLanguages().addAll(personLanguages);


        // Set activity characteristics
        person.setIsLargePoster(isLargePoster(person));
        return person;
    }

    private boolean isLargePoster(Person p) {
        return DateUtils.getMonth(p.getBirthday()) == Month.JANUARY;
    }

    private void resetState(int seed) {
        degreeDistribution.reset(seed);
        randomFarm.resetRandomGenerators(seed);
    }

    /**
     * Generates a block of persons
     *
     * @param blockId   Used as a seed to feed the pseudo-random number generators.
     * @param blockSize The size of the block of persons to generate.
     * @return person generator for at most blockSize persons.
     */
    public Iterator<Person> generatePersonBlock(int blockId, int blockSize) {
        resetState(blockId);
        nextId = blockId * blockSize;
        return new Iterator<Person>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < blockSize;
            }

            @Override
            public Person next() {
                ++i;
                return generatePerson();
            }
        };
    }
}

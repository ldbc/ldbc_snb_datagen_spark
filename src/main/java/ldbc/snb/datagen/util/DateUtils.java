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
package ldbc.snb.datagen.util;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.generator.tools.PowerDistribution;
import ldbc.snb.datagen.util.formatter.DateFormatter;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.TimeZone;

public class DateUtils {

    public static final long ONE_DAY = 24L * 60L * 60L * 1000L;
    public static final long SEVEN_DAYS = 7L * ONE_DAY;
    private static final long THIRTY_DAYS = 30L * ONE_DAY;
    private static final long ONE_YEAR = 365L * ONE_DAY;
    private static final long TWO_YEARS = 2L * ONE_YEAR;
    private static final long TEN_YEARS = 10L * ONE_YEAR;

    private long simulationStart;
    private long simulationEnd;
    private long fromBirthDay;
    private long toBirthDay;
    private GregorianCalendar calendar;
    private long bulkLoadThreshold;
    private PowerDistribution powerDist;
    private DateFormatter dateFormatter;

    // This constructor is for the case of friendship's created date generator
    public DateUtils(Config conf, GregorianCalendar simulationStartYear, GregorianCalendar simulationEndYear,
                     double alpha) {
        simulationEndYear.setTimeZone(TimeZone.getTimeZone("GMT"));
        simulationStartYear.setTimeZone(TimeZone.getTimeZone("GMT"));
        simulationStart = simulationStartYear.getTimeInMillis();
        simulationEnd = simulationEndYear.getTimeInMillis();
        powerDist = new PowerDistribution(0.0, 1.0, alpha);

        // For birthday from 1980 to 1990
        GregorianCalendar fromBirthCalendar = new GregorianCalendar(1980, 1, 1);
        fromBirthCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        GregorianCalendar toBirthCalendar = new GregorianCalendar(1990, 1, 1);
        toBirthCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        fromBirthDay = fromBirthCalendar.getTimeInMillis();
        toBirthDay = toBirthCalendar.getTimeInMillis();
        calendar = new GregorianCalendar();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        bulkLoadThreshold = getSimulationEnd() - (long) ((getSimulationEnd() - getSimulationStart()) * (DatagenParams.bulkLoadPortion));


        try {
            dateFormatter = DatagenParams.getDateFormatter();
            dateFormatter.initialize(conf);
        } catch (Exception e) {
            System.err.println("Error when initializing date formatter");
            System.err.println(e.getMessage());
        }
    }

    /**
     * Generate random Person creation date
     *
     * @param random random number generator
     * @return a random value on the interval [2010,2013]
     */
    public Long randomPersonCreationDate(Random random) {
        return (long) (simulationStart + random.nextDouble() * (simulationEnd - simulationStart));
    }

    /**
     * Generate random Person deletion date
     *
     * @param random       random number generator
     * @param creationDate Person creation date
     * @param maxNumKnows  maximum number of knows connections, influences the probability of leaving the network
     * @return a value on the interval [SS,SE]
     */
    public Long randomPersonDeletionDate(Random random, long creationDate, long maxNumKnows,long maxDeletionDate) {
        // TODO: use maxNumKnows to determine when a person's deleted
        long personCreationDate = creationDate + DatagenParams.delta;
        return randomDate(random, personCreationDate, maxDeletionDate);
    }

    /*
     * format the date
     */
    public String formatDate(long date) {
        return dateFormatter.formatDate(date);
    }

    public String formatYear(long date) {
        calendar.setTimeInMillis(date);
        int year = calendar.get(Calendar.YEAR);
        return year + "";
    }

    /*
     * format the date with hours and minutes
     */
    public String formatDateTime(long date) {
        return dateFormatter.formatDateTime(date);
    }


    public static boolean isTravelSeason(long date) {
        GregorianCalendar c = new GregorianCalendar();
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        c.setTimeInMillis(date);

        int day = c.get(Calendar.DAY_OF_MONTH);
        int month = c.get(Calendar.MONTH) + 1;

        if ((month > 4) && (month < 7)) {
            return true;
        }
        return ((month == 11) && (day > 23));
    }

    public int getNumberOfMonths(long date, int startMonth, int startYear) {
        calendar.setTimeInMillis(date);
        int month = calendar.get(Calendar.MONTH) + 1;
        int year = calendar.get(Calendar.YEAR);
        return (year - startYear) * 12 + month - startMonth;
    }

    public long randomKnowsCreationDate(Random random, Person personA, Person personB) {
        long fromDate = Math.max(personA.getCreationDate(), personB.getCreationDate()) + DatagenParams.delta;
        long toDate = Collections.min(Arrays.asList(personA.getDeletionDate(), personB.getDeletionDate(), simulationEnd));
        return randomDate(random, fromDate, toDate);
    }

    public long randomKnowsDeletionDate(Random random, Person personA, Person personB, long knowsCreationDate) {
        long fromDate = knowsCreationDate + DatagenParams.delta;
        long toDate = Collections.min(Arrays.asList(personA.getDeletionDate(), personB.getDeletionDate(), simulationEnd));
        return randomDate(random, fromDate, toDate);
    }

    public long numberOfMonths(Person person) {
        return numberOfMonths(person.getCreationDate());
    }

    public long numberOfMonths(long fromDate) {
        return (simulationEnd - fromDate) / THIRTY_DAYS;
    }

    public long randomDate(Random random, long minDate) {
        long maxDate = Math.max(minDate + THIRTY_DAYS, simulationEnd);
        return randomDate(random, minDate, maxDate);
    }

    public long randomDate(Random random, long minDate, long maxDate) {
        assert (minDate < maxDate) : "Invalid interval bounds. maxDate (" + maxDate + ") should be larger than minDate(" + minDate +")";
        return (long) (random.nextDouble() * (maxDate - minDate) + minDate);
    }

    /**
     * Returns the creation date of a comment following a power law distribution.
     *
     * @param random                 random number generator
     * @param lastCommentCreatedDate parent message creation date
     * @return creation date of replies
     */
    public long powerLawCommDateDay(Random random, long lastCommentCreatedDate) {
        return (long) (powerDist.getDouble(random) * ONE_DAY + lastCommentCreatedDate);
    }

    // The birthday is fixed during 1980 --> 1990
    public long getBirthDay(Random random) {
        calendar.setTimeInMillis(((long) (random.nextDouble() * (toBirthDay - fromBirthDay)) + fromBirthDay));
        GregorianCalendar aux_calendar = new GregorianCalendar(calendar.get(Calendar.YEAR), calendar
                .get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        aux_calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        return aux_calendar.getTimeInMillis();
    }

    public int getBirthYear(long birthday) {
        calendar.setTimeInMillis(birthday);
        return calendar.get(GregorianCalendar.YEAR);
    }

    public int getBirthMonth(long birthday) {
        calendar.setTimeInMillis(birthday);
        return calendar.get(GregorianCalendar.MONTH);
    }
    //If do not know the birthday, first randomly guess the age of person
    //Randomly get the age when person graduate
    //person's age for graduating is from 20 to 30

    public long getClassYear(Random random, long birthday) {
        long graduateAge = (random.nextInt(5) + 18) * ONE_YEAR;
        long classYear = birthday + graduateAge;
        if (classYear > this.simulationEnd) return -1;
        return classYear;
    }

    public long getWorkFromYear(Random random, long classYear, long birthday) {
        long workYear;
        if (classYear == -1) {
            long workingAge = 18 * ONE_YEAR;
            long from = birthday + workingAge;
            workYear = Math.min((long) (random.nextDouble() * (simulationEnd - from)) + from, simulationEnd);
        } else {
            workYear = (classYear + (long) (random.nextDouble() * TWO_YEARS));
        }
        return workYear;
    }

    public long getSimulationStart() {
        return simulationStart;
    }

    public long getSimulationEnd() {
        return simulationEnd;
    }

    public Long getNetworkCollapse() {
        return getSimulationStart() + DateUtils.TEN_YEARS;
    }

    public long getBulkLoadThreshold() {
        return bulkLoadThreshold;
    }

}

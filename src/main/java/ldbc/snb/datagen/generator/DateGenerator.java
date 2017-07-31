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

import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.serializer.formatter.DateFormatter;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

public class DateGenerator {

    public static final long ONE_DAY = 24L * 60L * 60L * 1000L;
    public static final long SEVEN_DAYS = 7L * ONE_DAY;
    public static final long THIRTY_DAYS = 30L * ONE_DAY;
    public static final long ONE_YEAR = 365L * ONE_DAY;
    public static final long TWO_YEARS = 2L * ONE_YEAR;
    public static final long TEN_YEARS = 10L * ONE_YEAR;
    public static final long THIRTY_YEARS = 30L * ONE_YEAR;

    private long from_;
    private long to_;
    private long fromBirthDay_;
    private long toBirthDay_;
    private GregorianCalendar calendar_;
    private long updateThreshold_;
    private PowerDistGenerator powerDist_;
    private DateFormatter dateFormatter_;

    // This constructor is for the case of friendship's created date generator
    public DateGenerator(Configuration conf, GregorianCalendar from, GregorianCalendar to,
                         double alpha) {
        to.setTimeZone(TimeZone.getTimeZone("GMT"));
        from.setTimeZone(TimeZone.getTimeZone("GMT"));
        from_ = from.getTimeInMillis();
        to_ = to.getTimeInMillis();
        powerDist_ = new PowerDistGenerator(0.0, 1.0, alpha);

        // For birthday from 1980 to 1990
        GregorianCalendar frombirthCalendar = new GregorianCalendar(1980, 1, 1);
        frombirthCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        GregorianCalendar tobirthCalendar = new GregorianCalendar(1990, 1, 1);
        tobirthCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        fromBirthDay_ = frombirthCalendar.getTimeInMillis();
        toBirthDay_ = tobirthCalendar.getTimeInMillis();
        calendar_ = new GregorianCalendar();
        calendar_.setTimeZone(TimeZone.getTimeZone("GMT"));
        updateThreshold_ = getEndDateTime() - (long) ((getEndDateTime() - getStartDateTime()) * (DatagenParams.updatePortion));

        try {
            dateFormatter_ = (DateFormatter) Class.forName(conf.get("ldbc.snb.datagen.serializer.dateFormatter"))
                                                  .newInstance();
            dateFormatter_.initialize(conf);
        } catch (Exception e) {
            System.err.println("Error when initializing date formatter");
            System.err.println(e.getMessage());
        }
    }

    /*
     * Date between from and to
     */
    public Long randomPersonCreationDate(Random random) {
        return (long) (random.nextDouble() * (to_ - from_) + from_);
    }

    /*
     * format the date
     */
    public String formatDate(long date) {
        return dateFormatter_.formatDate(date);
    }

    public String formatYear(long date) {
        calendar_.setTimeInMillis(date);
        int year = calendar_.get(Calendar.YEAR);
        return year + "";
    }

    /*
     * format the date with hours and minutes
     */
    public String formatDateTime(long date) {
        return dateFormatter_.formatDateTime(date);
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
        calendar_.setTimeInMillis(date);
        int month = calendar_.get(Calendar.MONTH) + 1;
        int year = calendar_.get(Calendar.YEAR);
        return (year - startYear) * 12 + month - startMonth;
    }

    public long randomKnowsCreationDate(Random random, Person personA, Person personB) {
        long fromDate = Math.max(personA.creationDate(), personB.creationDate()) + DatagenParams.deltaTime;
        return randomDate(random, fromDate, fromDate + THIRTY_DAYS);
    }

    public long numberOfMonths(Person user) {
        return numberOfMonths(user.creationDate());
    }

    public long numberOfMonths(long fromDate) {
        return (to_ - fromDate) / THIRTY_DAYS;
    }

    public long randomDate(Random random, long minDate) {
        long to = Math.max(minDate + THIRTY_DAYS, to_);
        return (long) (random.nextDouble() * (to - minDate) + minDate);
    }

    public long randomDate(Random random, long minDate, long maxDate) {
        long to = maxDate;
        return (long) (random.nextDouble() * (to - minDate) + minDate);
    }

    public long powerlawCommDateDay(Random random, long lastCommentCreatedDate) {
        long date = (long) (powerDist_.getDouble(random) * ONE_DAY + lastCommentCreatedDate);
        return date;
    }

    public long randomSevenDays(Random random) {
        return (long) (random.nextDouble() * DateGenerator.SEVEN_DAYS);
    }

    // The birthday is fixed during 1980 --> 1990
    public long getBirthDay(Random random, long userCreatedDate) {
        calendar_.setTimeInMillis(((long) (random.nextDouble() * (toBirthDay_ - fromBirthDay_)) + fromBirthDay_));
        GregorianCalendar aux_calendar = new GregorianCalendar(calendar_.get(Calendar.YEAR), calendar_
                .get(Calendar.MONTH), calendar_.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        aux_calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        return aux_calendar.getTimeInMillis();
    }

    public int getBirthYear(long birthDay) {
        calendar_.setTimeInMillis(birthDay);
        return calendar_.get(GregorianCalendar.YEAR);
    }

    public int getBirthMonth(long birthDay) {
        calendar_.setTimeInMillis(birthDay);
        return calendar_.get(GregorianCalendar.MONTH);
    }
    //If do not know the birthday, first randomly guess the age of user
    //Randomly get the age when user graduate
    //User's age for graduating is from 20 to 30

    public long getClassYear(Random random, long userCreatedDate, long birthday) {
        long graduateage = (random.nextInt(5) + 18) * ONE_YEAR;
        long classYear = birthday + graduateage;
        if (classYear > this.to_) return -1;
        return classYear;
    }

    public long getWorkFromYear(Random random, long classYear, long birthday) {
        long workYear = 0;
        if (classYear == -1) {
            //long workingage = (random.nextInt(10) + 25) * ONE_YEAR;
            long workingage = 18 * ONE_YEAR;
            long from = birthday + workingage;
            workYear = Math.min((long) (random.nextDouble() * (to_ - from)) + from, to_);
        } else {
            workYear = (classYear + (long) (random.nextDouble() * TWO_YEARS));
        }
        return workYear;
    }

    public long getStartDateTime() {
        return from_;
    }

    public long getEndDateTime() {
        return to_;
    }

    public long getUpdateThreshold() {
        return updateThreshold_;
    }
}

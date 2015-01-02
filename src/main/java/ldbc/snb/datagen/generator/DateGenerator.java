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
package ldbc.snb.datagen.generator;

import ldbc.snb.datagen.objects.Person;

import java.text.SimpleDateFormat;
import java.util.*;

public class DateGenerator {

	public static long ONE_DAY = 24L * 60L * 60L * 1000L;
	public static long SEVEN_DAYS = 7L * ONE_DAY;
	public static long THIRTY_DAYS = 30L * ONE_DAY;
	public static long ONE_YEAR = 365L * ONE_DAY;
	public static long TWO_YEARS = 2L * ONE_YEAR;
	public static long TEN_YEARS = 10L * ONE_YEAR;
	public static long THIRTY_YEARS = 30L * ONE_YEAR;

	private long from;
	private long to;
	private long fromBirthDay;
	private long toBirthDay;
	GregorianCalendar calendar;
	private long deltaTime;

	private PowerDistGenerator powerDist;

	// This constructor is for the case of friendship's created date generator
	public DateGenerator(GregorianCalendar from, GregorianCalendar to,
		double alpha, long deltaTime) {
		this.from = from.getTimeInMillis();
		this.to = to.getTimeInMillis();
		powerDist = new PowerDistGenerator(0.0, 1.0, alpha);
		this.deltaTime = deltaTime;

		// For birthday from 1980 to 1990
		GregorianCalendar frombirthCalendar = new GregorianCalendar(1980, 1, 1);
		GregorianCalendar tobirthCalendar = new GregorianCalendar(1990, 1, 1);
		this.fromBirthDay = frombirthCalendar.getTimeInMillis();
		this.toBirthDay = tobirthCalendar.getTimeInMillis();
		this.calendar = new GregorianCalendar();
		calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	/*
	 * Date between from and to
	 */
	public Long randomDateInMillis(Random random) {
		long date = (long) (random.nextDouble() * (to - from) + from);
		calendar.setTime(new Date(date));
		return calendar.getTimeInMillis();
	}

	/*
	 * format the date
	 */
	public String formatDate(long date) {
		SimpleDateFormat gmtDateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		gmtDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		calendar.setTimeInMillis(date);
		return gmtDateFormatter.format(calendar.getTime());
	}

	public String formatYear(long date) {
		calendar.setTimeInMillis(date);
		int year = calendar.get(Calendar.YEAR);
		return year + "";
	}

	/*
	 * format the date with hours and minutes
	 */
	public String formatDateDetail(long d) {
		calendar.setTimeInMillis(d);
		SimpleDateFormat gmtDateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		gmtDateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		return gmtDateFormatter.format(calendar.getTime());
	}


	public static boolean isTravelSeason(long date) {
		GregorianCalendar c = new GregorianCalendar();
		c.setTimeInMillis(date);

		int day = c.get(Calendar.DAY_OF_MONTH);
		int month = c.get(Calendar.MONTH) + 1;

		if ((month > 5) && (month < 8)) {
			return true;
		} else if ((month == 12) && (day > 23)) {
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Format date in xsd:dateTime format
	 */
/*	public String formatDateTime(Long date) {
		calendar.setTimeInMillis(date);
		String dateString = formatDate(calendar);
		return dateString + "T00:00:00";
	}

	public String formatDateTime(GregorianCalendar date) {
		String dateString = formatDate(date);
		return dateString + "T00:00:00";
	}
	*/

	public int getNumberOfMonths(GregorianCalendar c, int startMonth, int startYear) {
		int month = c.get(Calendar.MONTH) + 1;
		int year = c.get(Calendar.YEAR);

		return (year - startYear) * 12 + month - startMonth;
	}

	public Long randomDateInMillis(Random random, Long from, Long to) {
		long date = (long) (random.nextDouble() * (to - from) + from);
		calendar.setTime(new Date(date));

		return calendar.getTimeInMillis();
	}

	public Long randomThirtyDaysSpan(Random random, Long from) {
		long randomSpanMilis = (long) (random.nextDouble() * (THIRTY_DAYS));
		return (from + randomSpanMilis);
	}

	public long randomKnowsCreationDate(Random random, Person personA, Person personB) {
		long fromDate = Math.max(personA.creationDate(), personB.creationDate());
		long randomSpanMilis = (long) (random.nextDouble() * (THIRTY_DAYS));
		return (fromDate + randomSpanMilis);
	}

	public long numberOfMonths(Person user) {
		return numberOfMonths(user.creationDate());
	}

	public long numberOfMonths(long fromDate) {
		return (to - fromDate) / THIRTY_DAYS;
	}

	public long randomDate(Random random, long minDate) {
		return (long) (random.nextDouble() * (to - minDate) + minDate);
	}

	public long randomSevenDays(Random random) {
		return (long) (random.nextDouble() * DateGenerator.SEVEN_DAYS);
	}

	public long powerlawCommDateDay(Random random, long lastCommentCreatedDate) {
		return (long) (powerDist.getDouble(random) * ONE_DAY + lastCommentCreatedDate);
	}

	// The birthday is fixed during 1980 --> 1990
	public long getBirthDay(Random random, long userCreatedDate) {
		return (long) (random.nextDouble() * (toBirthDay - fromBirthDay) + fromBirthDay);
	}

	public int getBirthYear(long birthDay) {
		calendar.setTimeInMillis(birthDay);
		return calendar.get(GregorianCalendar.YEAR);
	}

	public int getBirthMonth(long birthDay) {
		calendar.setTimeInMillis(birthDay);
		return calendar.get(GregorianCalendar.MONTH);
	}
    //If do not know the birthday, first randomly guess the age of user
	//Randomly get the age when user graduate
	//User's age for graduating is from 20 to 30

	public long getClassYear(Random random, long userCreatedDate, long birthday) {
		long age;
		long graduateage = (random.nextInt(5) + 20) * ONE_YEAR;
		if (birthday != -1) {
			return (long) (birthday + graduateage);
		} else {
			age = (long) (random.nextDouble() * THIRTY_YEARS + TEN_YEARS);
			return (userCreatedDate - age + graduateage);
		}
	}

	public long getWorkFromYear(Random random, long userCreatedDate, long birthday) {
		long age;
		long workingage = (random.nextInt(10) + 25) * ONE_YEAR;
		if (birthday != -1) {
			return (long) (birthday + workingage);
		} else {
			age = (long) (random.nextDouble() * THIRTY_YEARS + TEN_YEARS);
			return (userCreatedDate - age + workingage);
		}
	}

	public long getWorkFromYear(Random random, long classYear) {
		return (classYear + (long) (random.nextDouble() * TWO_YEARS));
	}

	public long getStartDateTime() {
		return from;
	}

	public long getEndDateTime() {
		return to;
	}

	public long getMaxDateTime() {
		return to + SEVEN_DAYS + deltaTime;
	}
}

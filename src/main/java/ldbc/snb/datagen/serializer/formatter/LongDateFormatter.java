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
package ldbc.snb.datagen.serializer.formatter;

import org.apache.hadoop.conf.Configuration;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Created by aprat on 14/01/16.
 */
public class LongDateFormatter implements DateFormatter {
    private GregorianCalendar calendar_;
    private int minHour;
    private int minMinute;
    private int minSecond;
    private int minMillisecond;

    public void initialize(Configuration config) {
        calendar_ = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        minHour = calendar_.getActualMinimum(Calendar.HOUR);
        minMinute = calendar_.getActualMinimum(Calendar.MINUTE);
        minSecond = calendar_.getActualMinimum(Calendar.SECOND);
        minMillisecond = calendar_.getActualMinimum(Calendar.MILLISECOND);
    }

    public String formatDate(long date) {
        calendar_.setTimeInMillis(date);
        calendar_.set(Calendar.HOUR, minHour);
        calendar_.set(Calendar.MINUTE, minMinute);
        calendar_.set(Calendar.SECOND, minSecond);
        calendar_.set(Calendar.MILLISECOND, minMillisecond);
        return Long.toString(calendar_.getTimeInMillis());
    }

    public String formatDateTime(long date) {
        return Long.toString(date);
    }
}

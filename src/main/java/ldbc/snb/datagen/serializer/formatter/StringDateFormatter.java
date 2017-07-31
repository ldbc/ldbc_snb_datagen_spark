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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by aprat on 14/01/16.
 */
public class StringDateFormatter implements DateFormatter {

    private String formatDateTimeString_ = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private String formatDateString_ = "yyyy-MM-dd";

    private SimpleDateFormat gmtDateTimeFormatter_;
    private SimpleDateFormat gmtDateFormatter_;
    private Date date_;

    public void initialize(Configuration conf) {

        formatDateTimeString_ = conf
                .get("ldbc.snb.datagen.serializer.formatter.StringDateFormatter.dateTimeFormat", formatDateTimeString_);
        gmtDateTimeFormatter_ = new SimpleDateFormat(formatDateTimeString_);
        gmtDateTimeFormatter_.setTimeZone(TimeZone.getTimeZone("GMT"));
        formatDateString_ = conf
                .get("ldbc.snb.datagen.serializer.formatter.StringDateFormatter.dateFormat", formatDateString_);
        gmtDateFormatter_ = new SimpleDateFormat(formatDateString_);
        gmtDateFormatter_.setTimeZone(TimeZone.getTimeZone("GMT"));
        date_ = new Date();
    }

    public String formatDateTime(long date) {
        date_.setTime(date);
        return gmtDateTimeFormatter_.format(date_);
    }

    public String formatDate(long date) {
        date_.setTime(date);
        return gmtDateFormatter_.format(date_);
    }

}

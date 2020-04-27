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
package ldbc.snb.datagen.util.formatter;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.util.LdbcConfiguration;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class StringDateFormatter implements DateFormatter {

    private final ZoneId GMT = ZoneId.of("GMT");

    private String formatDateTimeString = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private String formatDateString = "yyyy-MM-dd";

    private DateTimeFormatter gmtDateTimeFormatter;
    private DateTimeFormatter gmtDateFormatter;

    public void initialize(LdbcConfiguration conf) {
        formatDateTimeString = DatagenParams.getDateTimeFormat();
        gmtDateTimeFormatter = DateTimeFormatter.ofPattern(formatDateTimeString).withZone(GMT);
        formatDateString = DatagenParams.getDateFormat();
        gmtDateFormatter = DateTimeFormatter.ofPattern(formatDateString).withZone(GMT);
    }

    public String formatDateTime(long date) {
        return gmtDateTimeFormatter.format(Instant.ofEpochMilli(date));
    }

    public String formatDate(long date) {
        return gmtDateFormatter.format(Instant.ofEpochMilli(date));
    }

}

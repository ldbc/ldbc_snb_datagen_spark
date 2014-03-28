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
package ldbc.socialnet.dbgen.serializer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by aprat on 3/27/14.
 */
public class UpdateEventSerializer {


    public enum UpdateEventType {
        ADD_PERSON,
        ADD_FRIENDSHIP,
        ADD_FORUM,
        ADD_MEMBERSHIP,
        ADD_POST,
        ADD_COMMENT,
        ADD_LIKE,
        ADD_INTEREST,
        ADD_WORKAT,
        ADD_STUDYAT,
        NO_EVENT
    }

    OutputStream fileOutputStream;

    public UpdateEventSerializer( String outputDir, String outputFileName, boolean compress ) {
        try{
            if( compress ) {
                this.fileOutputStream = new GZIPOutputStream(new FileOutputStream(outputDir + "/" + outputFileName +".gz"));
            } else {
                this.fileOutputStream = new FileOutputStream(outputDir + "/" + outputFileName );
            }
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void writeEvent( long date, UpdateEventType type, String data ) {
        String result = new String();
        result = result + Long.toString(date);
        result = result + ";";
        result = result + type.name();
        result = result + ";";
        result = result + data;
        try{
            fileOutputStream.write(result.getBytes());
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }

    public void close() {
        try {
            fileOutputStream.flush();
            fileOutputStream.close();
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}

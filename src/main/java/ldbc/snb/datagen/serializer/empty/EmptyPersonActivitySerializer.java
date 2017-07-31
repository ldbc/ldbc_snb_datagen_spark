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
package ldbc.snb.datagen.serializer.empty;

import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by aprat on 30/01/15.
 */
public class EmptyPersonActivitySerializer extends PersonActivitySerializer {

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        //This is left intentionally blank
    }

    @Override
    public void close() {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Forum forum) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Post post) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Comment comment) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Photo photo) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final ForumMembership membership) {
        //This is left intentionally blank
    }

    @Override
    protected void serialize(final Like like) {
        //This is left intentionally blank
    }

    @Override
    public void reset() {
        //This is left intentionally blank
    }
}

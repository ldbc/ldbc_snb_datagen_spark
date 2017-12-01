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
package ldbc.snb.datagen.serializer.snb.interactive;

import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author aprat
 */
public class CSVCompositePersonActivitySerializer extends PersonActivitySerializer {

    private CSVPersonActivitySerializer activitySerializer = new CSVPersonActivitySerializer();

    @Override
    public void initialize(Configuration conf, int reducerId) throws IOException {
        activitySerializer.initialize(conf, reducerId);
    }

    @Override
    public void close() {
        activitySerializer.close();
    }

    @Override
    protected void serialize(final Forum forum) {
        activitySerializer.serialize(forum);
    }

    @Override
    protected void serialize(final Post post) {
        activitySerializer.serialize(post);
    }

    @Override
    protected void serialize(final Comment comment) {
        activitySerializer.serialize(comment);
    }

    @Override
    protected void serialize(final Photo photo) {
        activitySerializer.serialize(photo);
    }

    @Override
    protected void serialize(final ForumMembership membership) {
        activitySerializer.serialize(membership);
    }

    @Override
    protected void serialize(final Like like) {
        activitySerializer.serialize(like);
    }

    @Override
    public void reset() {
        // Intentionally left empty
    }

}

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
package ldbc.snb.datagen.serializer.snb.csv.compositemergeforeign;

import ldbc.snb.datagen.entities.dynamic.messages.Comment;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.messages.Post;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.serializer.DynamicActivitySerializer;
import ldbc.snb.datagen.serializer.snb.csv.mergeforeign.CSVMergeForeignDynamicActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Created by aprat on 17/02/15.
 */
public class CSVCompositeMergeForeignDynamicActivitySerializer extends DynamicActivitySerializer {

    private CSVMergeForeignDynamicActivitySerializer activitySerializer = new CSVMergeForeignDynamicActivitySerializer();

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
        activitySerializer.export(forum);
    }

    @Override
    protected void serialize(final Post post) {
        activitySerializer.export(post);
    }

    @Override
    protected void serialize(final Comment comment) {
        activitySerializer.export(comment);
    }

    @Override
    protected void serialize(final Photo photo) {
        activitySerializer.export(photo);
    }

    @Override
    protected void serialize(final ForumMembership membership) {
        activitySerializer.export(membership);
    }

    @Override
    protected void serialize(final Like like) {
        activitySerializer.export(like);
    }

    @Override
    public void reset() {
        // Intentionally left empty
    }
}

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
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.objects.*;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * @author aprat
 */
abstract public class PersonActivitySerializer {


    public void export(final Forum forum) {
        serialize(forum);
    }

    public void export(final ForumMembership forumMembership) {
        serialize(forumMembership);
    }

    public void export(final Post post) {
        serialize(post);
    }

    public void export(Comment comment) {
        serialize(comment);

    }

    public void export(Photo photo) {
        serialize(photo);

    }

    public void export(Like like) {
        serialize(like);

    }


    abstract public void reset();

    abstract public void initialize(Configuration conf, int reducerId) throws IOException;

    abstract public void close();

    abstract protected void serialize(final Forum forum);

    abstract protected void serialize(final Post post);

    abstract protected void serialize(final Comment comment);

    abstract protected void serialize(final Photo photo);

    abstract protected void serialize(final ForumMembership membership);

    abstract protected void serialize(final Like like);


}

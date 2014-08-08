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
package ldbc.snb.datagen.serializer;

import ldbc.snb.datagen.objects.*;

/**
 * The empty serializer does nothing.
 * <p/>
 * Its purpose is to avoid the serializing I/O costs in debug phases.
 */
public class EmptySerializer implements Serializer {

    public EmptySerializer() {
    }

    public Long unitsGenerated() {
        return 0L;
    }

    @Override
    public void serialize(UserInfo info) {

    }

    @Override
    public void serialize(Friend friend) {

    }

    @Override
    public void serialize(Post post) {

    }

    @Override
    public void serialize(Like like) {

    }

    @Override
    public void serialize(Photo photo) {

    }

    @Override
    public void serialize(Comment comment) {

    }

    @Override
    public void serialize(Forum forum) {

    }

    @Override
    public void serialize(ForumMembership membership) {

    }

    @Override
    public void serialize(WorkAt workAt) {

    }

    @Override
    public void serialize(StudyAt studyAt) {

    }

    @Override
    public void serialize(Organization organization) {

    }

    @Override
    public void serialize(Tag tag) {

    }

    @Override
    public void serialize(Place place) {

    }

    @Override
    public void serialize(TagClass tagClass) {

    }

    public void close() {
    }

    public void resetState(long seed) {

    }
}

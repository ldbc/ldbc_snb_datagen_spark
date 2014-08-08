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
 * The ldbc socialnet generator serialize interface. The user of this interface has control of how the entities
 * are parsed with the gatherData methods.
 * <p/>
 * To ensure the correct serialization the close method must be called in the end so the serializer is able
 * to flush any non written data and close the files used.
 */
public interface Serializer {
    public void close();

    public Long unitsGenerated();

    public void serialize(UserInfo info);

    public void serialize(Friend friend);

    public void serialize(Post post);

    public void serialize(Like like);

    public void serialize(Photo photo);

    public void serialize(Comment comment);

    public void serialize(Forum forum);

    public void serialize(ForumMembership membership);

    public void serialize(WorkAt workAt);

    public void serialize(StudyAt studyAt);

    public void serialize(Organization organization);

    public void serialize(Tag tag);

    public void serialize(Place place);

    public void serialize(TagClass tagClass);
}
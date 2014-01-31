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

import ldbc.socialnet.dbgen.objects.Comment;
import ldbc.socialnet.dbgen.objects.Group;
import ldbc.socialnet.dbgen.objects.Photo;
import ldbc.socialnet.dbgen.objects.Post;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UserExtraInfo;

/**
 * The ldbc socialnet generator serialize interface. The user of this interface has control of how the entities
 * are parsed with the gatherData methods.
 * 
 * To ensure the correct serialization the close method must be called in the end so the serializer is able
 * to flush any non written data and close the files used.
 */
public interface Serializer {
	
    /**
     * Closes the serializer and flush the remaining data. Once called any GatherData method will produce I/O excpetions.
     */
	public void close();
	
	/**
	 * Returns how many serializer units (p.e.g rows in csv or triples in RDF) have been generated.
	 */
	public Long unitsGenerated();
	
	/**
	 * Serializes the user information.
	 * Aside from the user itself this includes:
	 *   · The location hierarchy of the user location and any university and country he has work with.
	 *   · The company and university data.
	 *   · The forum of this user wall.
	 *   · The tag data from its interests.
	 *   
	 * @param user: The user.
	 * @param extraInfo: The user cosmetic data.
	 */
	public void gatherData(ReducedUserProfile user, UserExtraInfo extraInfo);
	
	/**
	 * Serializes the post information.
	 * Aside from the post itself this includes:
	 *   · The location hierarchy of its location (via IP).
	 *   · Its tag data.
	 * 
	 * @param post: The post.
	 */
	public void gatherData(Post post);
	
	/**
     * Serializes the photo information.
     * Aside from the photo itself this includes:
     *   · The location hierarchy of its location (via IP).
     *   · Its tag data.
     * @param photo: The photo.
     */
	public void gatherData(Photo photo);
	
	/**
     * Serializes the comment information.
     * Aside from the comment itself this includes:
     *   · The location hierarchy of its location (via IP).
     *   · Its tag data.
     * @param comment: The comment.
     */
	public void gatherData(Comment comment);
	
	/**
     * Serializes the group information.
     * 
     * @param group: The group.
     */
	public void gatherData(Group group);
}
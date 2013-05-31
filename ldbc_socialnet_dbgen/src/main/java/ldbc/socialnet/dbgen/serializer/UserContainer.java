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

import java.util.HashSet;

import ldbc.socialnet.dbgen.objects.Friend;
import ldbc.socialnet.dbgen.objects.IP;
import ldbc.socialnet.dbgen.objects.ReducedUserProfile;
import ldbc.socialnet.dbgen.objects.UserProfile;


/**
 * Class used to hide the concrete UserProfile implementation. 
 */
public class UserContainer {
    private UserProfile user;
    private ReducedUserProfile reduced;
    UserContainer(UserProfile _user)
    {
        user = _user;
        reduced = null;
    }

    UserContainer(ReducedUserProfile _reduced)
    {
        reduced = _reduced;
        user = null;
    }

    public int getAccountId()
    {
        return (user == null) ? reduced.getAccountId() : user.getAccountId();
    }

    public long getBirthDay()
    {
        return (user == null) ? reduced.getBirthDay() : user.getBirthDay();
    }

    public byte getBrowserIdx()
    {
        return (user == null) ? reduced.getBrowserIdx() : user.getBrowserIdx();
    }

    public IP getIpAddress()
    {
        return (user == null) ? reduced.getIpAddress() : user.getIpAddress();
    }

    public int getForumWallId()
    {
        return (user == null) ? reduced.getForumWallId() : user.getForumWallId();
    }

    public int getForumStatusId()
    {
        return (user == null) ? reduced.getForumStatusId() : user.getForumStatusId();
    }

    public HashSet<Integer> getSetOfInterests()
    {
        return (user == null) ? reduced.getSetOfTags() : user.getSetOfTags();
    }

    public long getCreatedDate()
    {
        return (user == null) ? reduced.getCreatedDate() : user.getCreatedDate();
    }

    public Friend[] getFriendList()
    {
        return (user == null) ? reduced.getFriendList() : user.getFriendList();
    }
}

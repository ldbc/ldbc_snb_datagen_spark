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
package ldbc.snb.datagen.objects;

public class ForumMembership {
    long forumId;
    long userId;
    long joinDate;
    IP ip;
    int browserIdx;
    int agentIdx;
    boolean isLargePoster = false;       //This is used for creating large posts.
    boolean isFrequentChange;
    boolean isHaveSmartPhone;

    public long getForumId() {
        return forumId;
    }

    public void setForumId(long forumId) {
        this.forumId = forumId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public int getAgentIdx() {
        return agentIdx;
    }

    public void setAgentIdx(int agentIdx) {
        this.agentIdx = agentIdx;
    }

    public int getBrowserIdx() {
        return browserIdx;
    }

    public void setBrowserIdx(int browserIdx) {
        this.browserIdx = browserIdx;
    }

    public IP getIP() {
        return ip;
    }

    public void setIP(IP ip) {
        this.ip = ip;
    }

    public boolean isFrequentChange() {
        return isFrequentChange;
    }

    public void setFrequentChange(boolean isFrequentChange) {
        this.isFrequentChange = isFrequentChange;
    }

    public boolean isHaveSmartPhone() {
        return isHaveSmartPhone;
    }

    public void setHaveSmartPhone(boolean isHaveSmartPhone) {
        this.isHaveSmartPhone = isHaveSmartPhone;
    }

    public long getJoinDate() {
        return joinDate;
    }

    public void setJoinDate(long joinDate) {
        this.joinDate = joinDate;
    }

    public boolean isLargePoster() {
        return this.isLargePoster;
    }

    public void setLargePoster(boolean isLargePoster) {
        this.isLargePoster = isLargePoster;
    }
}

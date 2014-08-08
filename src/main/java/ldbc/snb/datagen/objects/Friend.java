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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Friend implements Serializable {
    private static final long serialVersionUID = 3657773293974543890L;
    long from;
    long to;
    long createdTime;            //approved Time
    long requestTime;
    long declinedTime;
    byte passIdx;
    byte initiator;            // 0: if user with smaller Id initiate the relationship, 1: if else


    //For user's agent information
    boolean isHaveSmartPhone;        // Use for providing the user agent information
    int agentIdx;                // Index of user agent in the dictionary, e.g., 0 for iPhone, 1 for HTC
    int browserIdx;                // Index of web browser, e.g., 0 for Internet Explorer

    boolean isFrequentChange;
    IP sourceIp;                // Source IP address of the friend

    boolean isLargePoster;            // True if friend is a large poster.


    public Friend(long from, long to, long _requestedTime, long _declinedTime, long _createdTime, byte passidx, byte initiator, boolean isLargePoster) {
        this.from = from;
        this.to = to;
        this.requestTime = _requestedTime;
        this.declinedTime = _declinedTime;
        this.createdTime = _createdTime;
        this.passIdx = passidx;
        this.initiator = initiator;
        this.isLargePoster = isLargePoster;
    }

    public Friend() {
    }

    public void readFields(DataInput arg0) throws IOException {
        from = arg0.readLong();
        to = arg0.readLong();
        createdTime = arg0.readLong();
        requestTime = arg0.readLong();
        declinedTime = arg0.readLong();
        passIdx = arg0.readByte();
        initiator = arg0.readByte();

        isHaveSmartPhone = arg0.readBoolean();
        agentIdx = arg0.readInt();
        browserIdx = arg0.readInt();
        isFrequentChange = arg0.readBoolean();
        int ip = arg0.readInt();
        int mask = arg0.readInt();
        this.sourceIp = new IP(ip, mask);
        this.isLargePoster = arg0.readBoolean();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(from);
        arg0.writeLong(to);
        arg0.writeLong(createdTime);
        arg0.writeLong(requestTime);
        arg0.writeLong(declinedTime);
        arg0.writeByte(passIdx);
        arg0.writeByte(initiator);

        arg0.writeBoolean(isHaveSmartPhone);
        arg0.writeInt(agentIdx);
        arg0.writeInt(browserIdx);
        arg0.writeBoolean(isFrequentChange);
        arg0.writeInt(sourceIp.getIp());
        arg0.writeInt(sourceIp.getMask());
        arg0.writeBoolean(isLargePoster);
    }

    public Friend(ReducedUserProfile from, ReducedUserProfile to, long _requestedTime, long _declinedTime, long _createdTime,
                  byte passidx, byte initiator) {
        this.from = from.getAccountId();
        this.to = to.getAccountId();
        this.requestTime = _requestedTime;
        this.declinedTime = _declinedTime;
        this.createdTime = _createdTime;
        this.passIdx = passidx;
        this.initiator = initiator;

        this.isHaveSmartPhone = to.isHaveSmartPhone();
        this.agentIdx = to.getAgentId();
        this.browserIdx = to.getBrowserId();
        this.isFrequentChange = to.isFrequentChange();
        this.setSourceIp(to.getIpAddress());
        this.isLargePoster = to.isLargePoster();
    }


    public long getFriendAcc() {
        return to;
    }

    public void setFriendAcc(long friendAcc) {
        this.to = friendAcc;
    }

    public long getUserAcc() {
        return from;
    }

    public void setUserAcc(long from) {
        this.from = from;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public int getPassIdx() {
        return passIdx;
    }

    public void setPassIdx(byte passIdx) {
        this.passIdx = passIdx;
    }

    public long getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    public long getDeclinedTime() {
        return declinedTime;
    }

    public void setDeclinedTime(long declinedTime) {
        this.declinedTime = declinedTime;
    }

    public byte getInitiator() {
        return initiator;
    }

    public void setInitiator(byte initiator) {
        this.initiator = initiator;
    }

    public IP getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(IP sourceIp) {
        this.sourceIp = sourceIp;
    }

    public boolean isHaveSmartPhone() {
        return isHaveSmartPhone;
    }

    public void setHaveSmartPhone(boolean isHaveSmartPhone) {
        this.isHaveSmartPhone = isHaveSmartPhone;
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

    public boolean isFrequentChange() {
        return isFrequentChange;
    }

    public void setFrequentChange(boolean isFrequentChange) {
        this.isFrequentChange = isFrequentChange;
    }

    public boolean isLargePoster() {
        return this.isLargePoster;
    }

    public void setLargePoster(boolean isLargePoster) {
        this.isLargePoster = isLargePoster;
    }

}

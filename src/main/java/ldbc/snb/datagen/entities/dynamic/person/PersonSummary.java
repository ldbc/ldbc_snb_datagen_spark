package ldbc.snb.datagen.entities.dynamic.person;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public final class PersonSummary implements Writable, Serializable {
    private long accountId;
    private long creationDate;
    private long deletionDate;
    private int browserId;
    private int country;
    private IP ipAddress;
    private boolean isLargePoster;
    private boolean isMessageDeleter;

    public PersonSummary() {
        ipAddress = new IP();
    }

    public PersonSummary(Person p) {
        accountId = p.getAccountId();
        creationDate = p.getCreationDate();
        deletionDate = p.getDeletionDate();
        browserId = p.getBrowserId();
        country = p.getCountryId();
        ipAddress = new IP(p.getIpAddress());
        isLargePoster = p.getIsLargePoster();
        isMessageDeleter = p.isMessageDeleter();
    }

    public PersonSummary(PersonSummary p) {
        accountId = p.getAccountId();
        creationDate = p.getCreationDate();
        deletionDate = p.getDeletionDate();
        browserId = p.getBrowserId();
        country = p.getCountryId();
        ipAddress = new IP(p.getIpAddress());
        isLargePoster = p.getIsLargePoster();
        isMessageDeleter = p.getIsMessageDeleter();
    }

    public void copy(PersonSummary p) {
        accountId = p.getAccountId();
        creationDate = p.getCreationDate();
        deletionDate = p.getDeletionDate();
        browserId = p.getBrowserId();
        country = p.getCountryId();
        ipAddress = new IP(p.getIpAddress());
        isLargePoster = p.getIsLargePoster();
        isMessageDeleter = p.getIsMessageDeleter();
    }

    public boolean getIsMessageDeleter() {
        return isMessageDeleter;
    }

    public long getAccountId() {
        return accountId;
    }

    public long getCreationDate() {
        return creationDate;
    }

    public long getDeletionDate() {
        return deletionDate;
    }

    public int getBrowserId() {
        return browserId;
    }

    public int getCountryId() {
        return country;
    }

    public IP getIpAddress() {
        return ipAddress;
    }

    public boolean getIsLargePoster() {
        return isLargePoster;
    }

    public void readFields(DataInput arg0) throws IOException {

        accountId = arg0.readLong();
        creationDate = arg0.readLong();
        deletionDate = arg0.readLong();
        browserId = arg0.readInt();
        country = arg0.readInt();
        ipAddress.readFields(arg0);
        isLargePoster = arg0.readBoolean();
        isMessageDeleter = arg0.readBoolean();
    }

    public void write(DataOutput arg0) throws IOException {
        arg0.writeLong(accountId);
        arg0.writeLong(creationDate);
        arg0.writeLong(deletionDate);
        arg0.writeInt(browserId);
        arg0.writeInt(country);
        ipAddress.write(arg0);
        arg0.writeBoolean(isLargePoster);
        arg0.writeBoolean(isMessageDeleter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersonSummary that = (PersonSummary) o;
        return accountId == that.accountId &&
                creationDate == that.creationDate &&
                deletionDate == that.deletionDate &&
                browserId == that.browserId &&
                country == that.country &&
                isLargePoster == that.isLargePoster &&
                isMessageDeleter == that.isMessageDeleter &&
                Objects.equals(ipAddress, that.ipAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, creationDate, deletionDate, browserId, country, ipAddress, isLargePoster, isMessageDeleter);
    }
}

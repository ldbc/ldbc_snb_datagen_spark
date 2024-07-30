package ldbc.snb.datagen.entities.statictype.tag;

import ldbc.snb.datagen.generator.dictionary.Dictionaries;

public class FlashMobTag implements Comparable<FlashMobTag> {
    public int level;
    public long date;
    public double prob;
    public int tag;

    public int compareTo(FlashMobTag t) {
        if (this.date - t.date < 0) return -1;
        if (this.date - t.date > 0) return 1;
        if (this.date - t.date == 0) return 0;
        return 0;
    }

    public void copyTo(FlashMobTag t) {
        t.level = this.level;
        t.date = this.date;
        t.prob = this.prob;
        t.tag = this.tag;
    }

    public String toString() {
        return "Level: " + level + " Date: " + date + " Tag:" + Dictionaries.tags.getName(tag);
    }
}

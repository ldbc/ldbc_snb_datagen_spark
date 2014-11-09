package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aprat on 11/9/14.
 */

    public class ComposedKey implements WritableComparable<ComposedKey> {
        public long block;
        public long key;

        public ComposedKey( ) {
        }

        public ComposedKey(ComposedKey cK) {
            this.block = cK.block;
            this.key = cK.key;
        }

        public ComposedKey( long block, long key) {
            this.block = block;
            this.key = key;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(block);
            out.writeLong(key);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            block = in.readLong();
            key = in.readLong();
        }

        @Override
        public int compareTo( ComposedKey mpk) {
            if (block < mpk.block) return -1;
            if (block > mpk.block) return 1;
            return 0;
        }
    }

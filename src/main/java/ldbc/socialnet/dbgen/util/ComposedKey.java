
package ldbc.socialnet.dbgen.util;

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
        public long id;

        public ComposedKey( ) {
        }

        public ComposedKey(ComposedKey cK) {
            this.block = cK.block;
            this.key = cK.key;
        }

        public ComposedKey( long block, long key, long id) {
            this.block = block;
            this.key = key;
            this.id = id;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(block);
            out.writeLong(key);
            out.writeLong(id);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            block = in.readLong();
            key = in.readLong();
            id = in.readLong();
        }

        @Override
        public int compareTo( ComposedKey mpk) {
            if (block < mpk.block) return -1;
            if (block > mpk.block) return 1;
            return 0;
        }
    }

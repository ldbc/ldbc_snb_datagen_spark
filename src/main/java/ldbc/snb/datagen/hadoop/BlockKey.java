package ldbc.snb.datagen.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by aprat on 11/9/14.
 */

    public class BlockKey implements WritableComparable<BlockKey> {
        public long block;
        public TupleKey tk;

        public BlockKey( ) {
            tk = new TupleKey();
        }

        public BlockKey(BlockKey bK) {
            this.block = bK.block;
            this.tk = new TupleKey(bK.tk);
        }

        public BlockKey( long block, TupleKey tk) {
            this.block = block;
            this.tk = tk;
        }

        public void write(DataOutput out) throws IOException {
            out.writeLong(block);
            tk.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            block = in.readLong();
            tk.readFields(in);
        }

        public int compareTo( BlockKey mpk) {
            if (block < mpk.block) return -1;
            if (block > mpk.block) return 1;
            return 0;
        }
    }
